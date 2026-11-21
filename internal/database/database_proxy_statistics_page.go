package database

import (
	"sort"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/evidence"
	"magpie/internal/platform/pg"

	"gorm.io/gorm"
)

const (
	proxyStatisticsDefaultPageSize = 20
	proxyStatisticsMaxPageSize     = 100
)

// GetProxyStatisticsPage returns one filtered, sorted page of a proxy's check
// history as this workspace sees it, with the totals the history card needs.
func GetProxyStatisticsPage(workspaceID uint, proxyID uint64, query dto.ProxyStatisticsQuery) (dto.ProxyStatisticsPage, error) {
	return getProxyStatisticsPageWithDB(pg.DB, workspaceID, proxyID, query, time.Now())
}

// NormaliseProxyStatisticsQuery clamps paging and drops unknown filter values,
// so a malformed query string falls back to the unfiltered first page.
func NormaliseProxyStatisticsQuery(query dto.ProxyStatisticsQuery) dto.ProxyStatisticsQuery {
	if query.Page < 1 {
		query.Page = 1
	}
	if query.PageSize < 1 {
		query.PageSize = proxyStatisticsDefaultPageSize
	}
	if query.PageSize > proxyStatisticsMaxPageSize {
		query.PageSize = proxyStatisticsMaxPageSize
	}
	switch query.Sort {
	case "response", "attempt", "protocol", "anonymity", "judge":
	default:
		query.Sort = "checked"
	}
	if query.Order != "asc" {
		query.Order = "desc"
	}
	if query.Status != "alive" && query.Status != "dead" {
		query.Status = ""
	}
	query.Protocol = strings.ToLower(strings.TrimSpace(query.Protocol))
	query.Transport = strings.ToLower(strings.TrimSpace(query.Transport))
	if query.Settings != "current" && query.Settings != "previous" {
		query.Settings = ""
	}
	switch query.Hours {
	case 1, 24, 168, 720:
	default:
		query.Hours = 0
	}
	return query
}

func getProxyStatisticsPageWithDB(db *gorm.DB, workspaceID uint, proxyID uint64, query dto.ProxyStatisticsQuery, now time.Time) (dto.ProxyStatisticsPage, error) {
	query = NormaliseProxyStatisticsQuery(query)
	page := dto.ProxyStatisticsPage{
		Statistics: []dto.ProxyStatistic{},
		Page:       query.Page,
		PageSize:   query.PageSize,
		Options:    dto.ProxyStatisticFilterLists{Protocols: []dto.ProxyStatisticProtocolOption{}, Judges: []dto.ProxyStatisticJudgeOption{}},
	}
	if proxyID == 0 {
		return page, nil
	}

	keys, err := currentStatisticConfigKeys(db, workspaceID, proxyID)
	if err != nil {
		return page, err
	}
	sqlite := db.Dialector.Name() == "sqlite"
	alive := statisticAliveExpr(sqlite)

	visible := func() *gorm.DB {
		q := db.Table("proxy_statistics").
			Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
			Where("proxy_statistics.proxy_id = ? AND up.workspace_id = ?", proxyID, workspaceID)
		return filterWorkspaceStatisticEvidence(q, workspaceID)
	}
	filtered := func(withStatus bool) *gorm.DB {
		q := visible()
		if query.Protocol != "" {
			q = q.Where("proxy_statistics.protocol_id IN (SELECT id FROM protocols WHERE name = ?)", query.Protocol)
		}
		if query.Transport != "" {
			q = q.Where("proxy_statistics.transport_protocol = ?", query.Transport)
		}
		if query.JudgeID != 0 {
			q = q.Where("proxy_statistics.judge_id = ?", query.JudgeID)
		}
		if query.Hours > 0 {
			q = q.Where("proxy_statistics.created_at >= ?", now.Add(-time.Duration(query.Hours)*time.Hour).UTC())
		}
		if query.Settings != "" {
			current, vars := statisticCurrentConfigExpr(sqlite, workspaceID, keys)
			if query.Settings == "previous" {
				current = "NOT (" + current + ")"
			}
			q = q.Where(current, vars...)
		}
		if withStatus && query.Status != "" {
			if query.Status == "alive" {
				q = q.Where(alive, workspaceID)
			} else {
				q = q.Where("NOT ("+alive+")", workspaceID)
			}
		}
		return q
	}

	// Counts ignore the status filter: the status buttons show what the other filters leave.
	var counts struct {
		Alive int64
		Dead  int64
	}
	verdicts := filtered(false).Select(alive+" AS ok", workspaceID)
	if err := db.Table("(?) AS verdicts", verdicts).
		Select("COALESCE(SUM(CASE WHEN ok THEN 1 ELSE 0 END), 0) AS alive, COALESCE(SUM(CASE WHEN ok THEN 0 ELSE 1 END), 0) AS dead").
		Scan(&counts).Error; err != nil {
		return page, err
	}
	page.Counts = dto.ProxyStatisticCounts{Alive: counts.Alive, Dead: counts.Dead}
	switch query.Status {
	case "alive":
		page.Total = counts.Alive
	case "dead":
		page.Total = counts.Dead
	default:
		page.Total = counts.Alive + counts.Dead
	}

	if err := loadStatisticFilterOptions(visible, &page.Options); err != nil {
		return page, err
	}
	if page.Total == 0 || int64((query.Page-1)*query.PageSize) >= page.Total {
		return page, nil
	}

	// Page of ids in display order, then the full rows for just that page.
	ordered := filtered(true).
		Joins("LEFT JOIN protocols pr ON pr.id = proxy_statistics.protocol_id").
		Joins("LEFT JOIN anonymity_levels al ON al.id = proxy_statistics.level_id").
		Joins("LEFT JOIN judges jd ON jd.id = proxy_statistics.judge_id")
	if query.Sort == "response" {
		ordered = ordered.Select("proxy_statistics.id AS id, "+alive+" AS ok", workspaceID)
	} else {
		ordered = ordered.Select("proxy_statistics.id AS id")
	}
	var idRows []struct{ ID uint64 }
	if err := orderProxyStatistics(ordered, query).Limit(query.PageSize).Offset((query.Page - 1) * query.PageSize).Scan(&idRows).Error; err != nil {
		return page, err
	}
	ids := make([]uint64, 0, len(idRows))
	for _, row := range idRows {
		ids = append(ids, row.ID)
	}
	if len(ids) == 0 {
		return page, nil
	}
	var rows []domain.ProxyStatistic
	if err := db.Model(&domain.ProxyStatistic{}).Preload("Protocol").Preload("Level").Preload("Judge").
		Where("id IN ?", ids).Find(&rows).Error; err != nil {
		return page, err
	}
	position := make(map[uint64]int, len(ids))
	for i, id := range ids {
		position[id] = i
	}
	sort.Slice(rows, func(i, j int) bool { return position[rows[i].ID] < position[rows[j].ID] })
	page.Statistics = workspaceStatistics(rows, workspaceID, keys)
	return page, nil
}

// orderProxyStatistics applies the history card's sort. Checks without a latency
// (dead checks, whose value is the timeout) and unknown anonymity sort last in
// both directions; the check time breaks ties, newest first.
// The response sort expects the query to select the workspace verdict as "ok".
func orderProxyStatistics(q *gorm.DB, query dto.ProxyStatisticsQuery) *gorm.DB {
	dir := " DESC"
	if query.Order == "asc" {
		dir = " ASC"
	}
	switch query.Sort {
	case "response":
		q = q.Order("ok DESC").Order("proxy_statistics.response_time" + dir)
	case "attempt":
		q = q.Order("proxy_statistics.attempt" + dir)
	case "protocol":
		q = q.Order("pr.name" + dir).Order("proxy_statistics.transport_protocol" + dir)
	case "anonymity":
		q = q.Order("CASE WHEN al.name IN ('transparent', 'anonymous', 'elite') THEN 0 ELSE 1 END").
			Order("CASE al.name WHEN 'transparent' THEN 0 WHEN 'anonymous' THEN 1 WHEN 'elite' THEN 2 ELSE 3 END" + dir)
	case "judge":
		q = q.Order("REPLACE(REPLACE(jd.full_string, 'https://', ''), 'http://', '')" + dir).Order("jd.full_string" + dir)
	default:
		return q.Order("proxy_statistics.created_at" + dir).Order("proxy_statistics.id" + dir)
	}
	return q.Order("proxy_statistics.created_at DESC").Order("proxy_statistics.id DESC")
}

func loadStatisticFilterOptions(visible func() *gorm.DB, options *dto.ProxyStatisticFilterLists) error {
	var protocols []dto.ProxyStatisticProtocolOption
	if err := visible().
		Joins("JOIN protocols pr ON pr.id = proxy_statistics.protocol_id").
		Distinct("pr.name AS protocol", "proxy_statistics.transport_protocol AS transport").
		Order("protocol").Order("transport").
		Scan(&protocols).Error; err != nil {
		return err
	}
	var judges []dto.ProxyStatisticJudgeOption
	if err := visible().
		Joins("JOIN judges jd ON jd.id = proxy_statistics.judge_id").
		Distinct("jd.id AS id", "jd.full_string AS url").
		Order("url").
		Scan(&judges).Error; err != nil {
		return err
	}
	if protocols != nil {
		options.Protocols = protocols
	}
	if judges != nil {
		options.Judges = judges
	}
	return nil
}

// statisticAliveExpr is this workspace's verdict for a check: its own evidence
// when the row carries attribution, otherwise the physical result. It takes the
// workspace id as its only parameter.
func statisticAliveExpr(sqlite bool) string {
	if sqlite {
		return "CASE WHEN COALESCE(json_array_length(proxy_statistics.check_evidence), 0) = 0 THEN proxy_statistics.alive " +
			"ELSE EXISTS (SELECT 1 FROM json_each(proxy_statistics.check_evidence) e WHERE json_extract(e.value, '$.workspace_id') = ? AND json_extract(e.value, '$.alive') = 1) END"
	}
	return "CASE WHEN COALESCE(jsonb_array_length(proxy_statistics.check_evidence), 0) = 0 THEN proxy_statistics.alive " +
		"ELSE proxy_statistics.check_evidence @> jsonb_build_array(jsonb_build_object('workspace_id', CAST(? AS bigint), 'alive', true)) END"
}

// statisticCurrentConfigExpr matches checks made with a configuration that is
// still current for their protocol in this workspace. Legacy rows have no
// configuration key and never match.
func statisticCurrentConfigExpr(sqlite bool, workspaceID uint, keys map[int]string) (string, []any) {
	if len(keys) == 0 {
		return "1 = 0", nil
	}
	protocols := make([]int, 0, len(keys))
	for protocolID := range keys {
		protocols = append(protocols, protocolID)
	}
	sort.Ints(protocols)
	parts := make([]string, 0, len(protocols))
	vars := make([]any, 0, len(protocols)*3)
	for _, protocolID := range protocols {
		if sqlite {
			parts = append(parts, "(proxy_statistics.protocol_id = ? AND EXISTS (SELECT 1 FROM json_each(proxy_statistics.check_evidence) e WHERE json_extract(e.value, '$.workspace_id') = ? AND json_extract(e.value, '$.config_key') = ?))")
		} else {
			parts = append(parts, "(proxy_statistics.protocol_id = ? AND proxy_statistics.check_evidence @> jsonb_build_array(jsonb_build_object('workspace_id', CAST(? AS bigint), 'config_key', CAST(? AS text))))")
		}
		vars = append(vars, protocolID, workspaceID, keys[protocolID])
	}
	return "(" + strings.Join(parts, " OR ") + ")", vars
}

// currentStatisticConfigKeys maps each protocol to the configuration key of
// this workspace's current check for the proxy.
func currentStatisticConfigKeys(db *gorm.DB, workspaceID uint, proxyID uint64) (map[int]string, error) {
	var current []domain.ProxyLatestStatistic
	if err := evidence.CurrentLatestStatistics(db, workspaceID).Where("pls.proxy_id = ?", proxyID).Find(&current).Error; err != nil {
		return nil, err
	}
	keys := make(map[int]string, len(current))
	for _, latest := range current {
		keys[latest.ProtocolID] = latest.ConfigKey
	}
	return keys, nil
}

// workspaceStatistics maps stored checks to this workspace's view: its verdict
// and configuration key from the attribution, and whether that configuration
// is still current.
func workspaceStatistics(rows []domain.ProxyStatistic, workspaceID uint, keys map[int]string) []dto.ProxyStatistic {
	stats := make([]dto.ProxyStatistic, 0, len(rows))
	for index := range rows {
		stat := mapProxyStatistic(&rows[index])
		for _, evidence := range rows[index].CheckEvidence {
			if evidence.WorkspaceID == workspaceID {
				stat.Alive, stat.ConfigKey = evidence.Alive, evidence.ConfigKey
				break
			}
		}
		key, checked := keys[rows[index].ProtocolID]
		stat.Current = checked && key == stat.ConfigKey
		stats = append(stats, stat)
	}
	return stats
}
