package database

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

func GetProxyDetail(workspaceID uint, proxyId uint64) (*dto.ProxyDetail, error) {
	if proxyId == 0 {
		return nil, nil
	}

	var proxy domain.Proxy
	err := DB.
		Preload("Reputations").
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id").
		Where("up.workspace_id = ? AND proxies.id = ?", workspaceID, proxyId).
		First(&proxy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := hydrateProxyCredential(DB, &proxy, workspaceID); err != nil {
		return nil, err
	}

	var latestStat *dto.ProxyStatistic
	var latestCheck *time.Time
	var current []domain.ProxyLatestStatistic
	if err := currentLatestStatistics(DB, workspaceID).Where("pls.proxy_id = ?", proxyId).Order("pls.checked_at DESC, pls.statistic_id DESC").Find(&current).Error; err != nil {
		return nil, err
	}
	var alive *bool
	if len(current) > 0 {
		value := false
		for _, check := range current {
			value = value || check.Alive
		}
		alive = &value
		latestCheck = &current[0].CheckedAt
		var stat domain.ProxyStatistic
		if err := DB.Preload("Protocol").Preload("Level").Preload("Judge").Where("id = ? AND created_at = ?", current[0].StatisticID, current[0].CheckedAt).First(&stat).Error; err == nil {
			mapped := mapProxyStatistic(&stat)
			mapped.Alive, mapped.ConfigKey, mapped.Current = current[0].Alive, current[0].ConfigKey, true
			latestStat = &mapped
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	detail := &dto.ProxyDetail{
		Alive:           alive,
		Id:              int(proxy.ID),
		IP:              proxy.GetIp(),
		Port:            proxy.Port,
		Username:        proxy.Username,
		Password:        proxy.Password,
		HasAuth:         proxy.HasAuth(),
		EstimatedType:   normaliseDisplayValue(proxy.EstimatedType, "N/A"),
		Country:         normaliseDisplayValue(proxy.Country, "Unknown"),
		CreatedAt:       proxy.CreatedAt,
		LatestCheck:     latestCheck,
		LatestStatistic: latestStat,
	}

	detail.Reputation = mapReputationsToBreakdown(proxy.Reputations)
	var managed domain.ManagedProxy
	if managedErr := DB.Select("state", "pause_reason").
		Where("workspace_id = ? AND proxy_id = ?", workspaceID, proxyId).
		First(&managed).Error; managedErr == nil {
		detail.State = managed.State
		detail.PauseReason = managed.PauseReason
	}
	detail.Tags, err = getProxyTagsForProxy(workspaceID, proxyId)
	if err != nil {
		return nil, err
	}

	return detail, nil
}

func GetQueuedProxyForWorkspace(workspaceID uint, proxyId uint64) (*domain.Proxy, error) {
	if proxyId == 0 {
		return nil, nil
	}

	var proxy domain.Proxy
	err := DB.
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id").
		Where("up.workspace_id = ? AND up.state = ? AND proxies.id = ?", workspaceID, domain.ManagedProxyStateActive, proxyId).
		First(&proxy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	items := []domain.Proxy{proxy}
	if err := hydrateActiveProxyWorkspaces(DB, items); err != nil {
		return nil, err
	}
	proxy = items[0]
	if err := hydrateProxyCredential(DB, &proxy, workspaceID); err != nil {
		return nil, err
	}

	return &proxy, nil
}

func GetProxyQueueState(proxyID uint64) (*domain.Proxy, bool, error) {
	if proxyID == 0 {
		return nil, false, nil
	}
	var proxy domain.Proxy
	if err := DB.First(&proxy, proxyID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	items := []domain.Proxy{proxy}
	if err := hydrateActiveProxyWorkspaces(DB, items); err != nil {
		return nil, false, err
	}
	proxy = items[0]
	if err := hydrateProxyCredential(DB, &proxy, 0); err != nil {
		return nil, false, err
	}
	return &proxy, len(proxy.Workspaces) > 0, nil
}

func GetProxyStatistics(workspaceID uint, proxyId uint64, limit int) ([]dto.ProxyStatistic, error) {
	if proxyId == 0 {
		return []dto.ProxyStatistic{}, nil
	}

	if limit <= 0 || limit > 500 {
		limit = 500
	}

	query := DB.Model(&domain.ProxyStatistic{}).
		Preload("Protocol").
		Preload("Level").
		Preload("Judge").
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Where("proxy_statistics.proxy_id = ? AND up.workspace_id = ?", proxyId, workspaceID).
		Order("proxy_statistics.created_at DESC").
		Limit(limit)

	query = filterWorkspaceStatisticEvidence(query, workspaceID)
	rows := make([]domain.ProxyStatistic, 0, limit)
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	var current []domain.ProxyLatestStatistic
	if err := currentLatestStatistics(DB, workspaceID).Where("pls.proxy_id = ?", proxyId).Find(&current).Error; err != nil {
		return nil, err
	}
	keys := make(map[int]string, len(current))
	for _, latest := range current {
		keys[latest.ProtocolID] = latest.ConfigKey
	}
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

	return stats, nil
}

type proxyStatisticBodyRow struct {
	ResponseBody string
	Regex        sql.NullString
}

func GetProxyStatisticResponseBody(workspaceID uint, proxyId uint64, statisticId uint64) (dto.ProxyStatisticDetail, error) {
	if proxyId == 0 || statisticId == 0 {
		return dto.ProxyStatisticDetail{}, gorm.ErrRecordNotFound
	}

	var row proxyStatisticBodyRow
	query := DB.Table("proxy_statistics").
		Select("proxy_statistics.response_body", "user_judges.regex").
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Joins("LEFT JOIN user_judges ON user_judges.judge_id = proxy_statistics.judge_id AND user_judges.workspace_id = up.workspace_id").
		Where("proxy_statistics.id = ? AND proxy_statistics.proxy_id = ? AND up.workspace_id = ?", statisticId, proxyId, workspaceID).
		Session(&gorm.Session{})
	err := filterWorkspaceStatisticEvidence(query, workspaceID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dto.ProxyStatisticDetail{}, gorm.ErrRecordNotFound
		}
		return dto.ProxyStatisticDetail{}, err
	}

	regex := ""
	if row.Regex.Valid {
		regex = strings.TrimSpace(row.Regex.String)
	}

	return dto.ProxyStatisticDetail{
		ResponseBody: row.ResponseBody,
		Regex:        regex,
	}, nil
}

func mapProxyStatistic(stat *domain.ProxyStatistic) dto.ProxyStatistic {
	if stat == nil {
		return dto.ProxyStatistic{}
	}

	protocol := normaliseDisplayValue(stat.Protocol.Name, "Unknown")
	anonymity := normaliseDisplayValue(stat.Level.Name, "Unknown")
	judge := normaliseDisplayValue(stat.Judge.FullString, "Unknown")

	return dto.ProxyStatistic{
		Transport: stat.TransportProtocol, Timeout: stat.CheckTimeout, Retries: stat.CheckRetries,
		Id:             stat.ID,
		Alive:          stat.Alive,
		Attempt:        stat.Attempt,
		ResponseTime:   stat.ResponseTime,
		ResponseBody:   stat.ResponseBody,
		Protocol:       protocol,
		AnonymityLevel: anonymity,
		Judge:          judge,
		CreatedAt:      stat.CreatedAt,
	}
}

func normaliseDisplayValue(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
