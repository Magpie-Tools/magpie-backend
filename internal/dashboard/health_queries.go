package dashboard

import (
	"database/sql"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/evidence"

	"gorm.io/gorm"
)

type recentProxyCheckRow struct {
	Ordinal      int          `gorm:"column:ordinal"`
	ID           uint64       `gorm:"column:id"`
	IPAddress    string       `gorm:"column:ip_address"`
	Port         uint16       `gorm:"column:port"`
	ResponseTime uint16       `gorm:"column:response_time"`
	Alive        bool         `gorm:"column:alive"`
	LatestCheck  sql.NullTime `gorm:"column:latest_check"`
}

// queryRecentProxyChecks selects the first limit active routes in the read
// model's recency order, with Ordinal their position in that order, then
// reads their current evidence and returns them in the final order: alive
// first, latest check with missing checks last, then route ID.
func queryRecentProxyChecks(db *gorm.DB, workspaceID uint, limit int) ([]recentProxyCheckRow, error) {
	rows := make([]recentProxyCheckRow, 0, limit)
	query := `
WITH candidates AS MATERIALIZED (
	SELECT
		ufi.proxy_id AS id,
		ufi.workspace_id,
		ufi.host AS ip_address,
		ufi.port,
		ufi.alive,
		ufi.latest_check AS last_checked_at
	FROM user_proxy_filter_indexes ufi
	JOIN workspaces cw ON cw.id = ufi.workspace_id AND NOT cw.checker_dirty
	WHERE ufi.workspace_id = ? AND ufi.state = 'active'
	ORDER BY ufi.alive DESC, ufi.latest_check DESC, ufi.proxy_id ASC
	LIMIT ?
), ordered AS MATERIALIZED (
	SELECT candidates.*, ROW_NUMBER() OVER (ORDER BY candidates.alive DESC, candidates.last_checked_at DESC, candidates.id ASC) AS ordinal
	FROM candidates
)
SELECT
	c.ordinal,
	c.id,
	c.ip_address,
	c.port,
	COALESCE(latest.response_time, 0) AS response_time,
	COALESCE(pos.overall_alive,FALSE) AS alive,
	COALESCE(pos.last_checked_at, latest.checked_at) AS latest_check
FROM ordered c
LEFT JOIN LATERAL (` + evidence.CurrentOverallStatusForProxySQL("c.workspace_id", "c.id") + `) pos ON TRUE
LEFT JOIN LATERAL (
	SELECT pls.response_time, pls.checked_at
	FROM (` + evidence.CurrentLatestStatisticsSQL + `) pls
	WHERE pls.proxy_id = c.id AND pls.checking_workspace_id = c.workspace_id
	ORDER BY pls.checked_at DESC, pls.statistic_id DESC
	LIMIT 1
) latest ON TRUE
ORDER BY alive DESC, latest_check DESC NULLS LAST, c.id ASC
`
	if err := db.Raw(query, workspaceID, limit).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// queryFastestAliveProxies ranks the workspace's alive current evidence: latest
// evidence per route, protocol and current config key (sparse plan overrides
// and the generation-0 path included), actively owned routes only, the best
// successful response time per route, ties broken by the latest check and
// then the route ID. ADR 0017 fixes these semantics for any rewrite.
func queryFastestAliveProxies(db *gorm.DB, workspaceID uint, limit int) ([]dto.ProxyFastestAlive, error) {
	latestAliveStats := evidence.DashboardLatestStatistics(db, workspaceID).
		Select("DISTINCT ON (pls.proxy_id) pls.proxy_id, pls.response_time, pls.checked_at").
		Where("pls.alive = ?", true).
		Order("pls.proxy_id, pls.response_time ASC, pls.checked_at DESC, pls.statistic_id DESC")
	fastestCandidates := db.Table("(?) latest", latestAliveStats).
		Select("latest.proxy_id, latest.response_time, latest.checked_at").
		Order("latest.response_time ASC, latest.checked_at DESC, latest.proxy_id ASC").
		Limit(limit)

	type fastestAliveProxyRow struct {
		ID              uint64       `gorm:"column:id"`
		IPAddress       string       `gorm:"column:ip_address"`
		Port            uint16       `gorm:"column:port"`
		ResponseTime    uint16       `gorm:"column:response_time"`
		Country         string       `gorm:"column:country"`
		ReputationLabel string       `gorm:"column:reputation_label"`
		ReputationScore float32      `gorm:"column:reputation_score"`
		LatestCheck     sql.NullTime `gorm:"column:latest_check"`
	}

	rows := make([]fastestAliveProxyRow, 0, limit)
	// The route and reputation joins do not affect eligibility or ordering.
	// Enrich only the winning candidates, rather than every alive route.
	if err := db.Raw(`WITH candidates AS MATERIALIZED (?)
SELECT proxies.id AS id, proxies.host AS ip_address, proxies.port,
 COALESCE(latest.response_time, 0) AS response_time,
 COALESCE(NULLIF(proxies.country, ''), 'N/A') AS country,
 LOWER(COALESCE(NULLIF(pr.label, ''), 'unknown')) AS reputation_label,
 COALESCE(pr.score, 0) AS reputation_score, latest.checked_at AS latest_check
FROM candidates latest
JOIN proxies ON proxies.id = latest.proxy_id
LEFT JOIN proxy_reputations pr ON pr.proxy_id = proxies.id AND pr.kind = ?
ORDER BY latest.response_time ASC, latest.checked_at DESC, proxies.id ASC`, fastestCandidates, domain.ProxyReputationKindOverall).
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]dto.ProxyFastestAlive, 0, len(rows))
	for _, row := range rows {
		latestCheck := time.Time{}
		if row.LatestCheck.Valid {
			latestCheck = row.LatestCheck.Time
		}

		result = append(result, dto.ProxyFastestAlive{
			ID:              row.ID,
			IP:              row.IPAddress,
			Port:            row.Port,
			ResponseTime:    row.ResponseTime,
			Country:         row.Country,
			ReputationLabel: row.ReputationLabel,
			ReputationScore: row.ReputationScore,
			LatestCheck:     latestCheck,
		})
	}
	return result, nil
}

type judgeBreakdownRow struct {
	JudgeUrl           string `json:"judge_url"`
	EliteProxies       uint   `json:"elite_proxies"`
	AnonymousProxies   uint   `json:"anonymous_proxies"`
	TransparentProxies uint   `json:"transparent_proxies"`
}

// queryJudgeBreakdown counts the workspace's alive current evidence per
// assigned judge and anonymity level.
func queryJudgeBreakdown(db *gorm.DB, workspaceID uint) ([]judgeBreakdownRow, error) {
	var judges []judgeBreakdownRow
	err := evidence.DashboardLatestStatistics(db, workspaceID).
		Select(
			"j.full_string AS judge_url, "+
				"SUM(CASE WHEN al.name = 'elite' THEN 1 ELSE 0 END)       AS elite_proxies, "+
				"SUM(CASE WHEN al.name = 'anonymous' THEN 1 ELSE 0 END)   AS anonymous_proxies, "+
				"SUM(CASE WHEN al.name = 'transparent' THEN 1 ELSE 0 END) AS transparent_proxies",
		).
		Joins("JOIN user_judges uj ON uj.judge_id = pls.judge_id AND uj.workspace_id = ?", workspaceID).
		Joins("JOIN judges j ON j.id = pls.judge_id").
		Joins("JOIN anonymity_levels al ON al.id = pls.level_id").
		Where("pls.alive = TRUE").
		Group("j.id, j.full_string").
		Scan(&judges).Error
	return judges, err
}
