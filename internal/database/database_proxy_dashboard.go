package database

import (
	"database/sql"
	"sync"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

const defaultRecentProxyChecksLimit = 8

const maxRecentProxyChecksLimit = 50

const dashboardFastestAliveLimit = 100

type dashboardProxyListCacheKey struct {
	WorkspaceID uint
	Limit       int
}

var dashboardRecentChecksCache sync.Map

var dashboardFastestAliveCache sync.Map

func GetRecentProxyChecks(workspaceID uint, limit int) []dto.ProxyRecentCheck {
	version := currentCheckerHealthVersion(workspaceID)
	if version.Dirty {
		return []dto.ProxyRecentCheck{}
	}
	if limit <= 0 {
		limit = defaultRecentProxyChecksLimit
	} else if limit > maxRecentProxyChecksLimit {
		limit = maxRecentProxyChecksLimit
	}

	key := dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: limit}
	if cached, ok := dashboardRecentChecksCache.Load(key); ok {
		entry := cached.(dashboardHealthCacheEntry[[]dto.ProxyRecentCheck])
		if entry.version == version {
			return entry.value
		}
	}

	return RefreshRecentProxyChecksCache(workspaceID, limit)
}

func RefreshRecentProxyChecksCache(workspaceID uint, limit int) []dto.ProxyRecentCheck {
	version := currentCheckerHealthVersion(workspaceID)
	if limit <= 0 {
		limit = defaultRecentProxyChecksLimit
	} else if limit > maxRecentProxyChecksLimit {
		limit = maxRecentProxyChecksLimit
	}

	type recentProxyCheckRow struct {
		ID           uint64       `gorm:"column:id"`
		IPAddress    string       `gorm:"column:ip_address"`
		Port         uint16       `gorm:"column:port"`
		ResponseTime uint16       `gorm:"column:response_time"`
		Alive        bool         `gorm:"column:alive"`
		LatestCheck  sql.NullTime `gorm:"column:latest_check"`
	}

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
)
SELECT
	c.id,
	c.ip_address,
	c.port,
	COALESCE(latest.response_time, 0) AS response_time,
	COALESCE(pos.overall_alive,FALSE) AS alive,
	COALESCE(pos.last_checked_at, latest.checked_at) AS latest_check
FROM candidates c
LEFT JOIN LATERAL (` + currentOverallStatusForProxySQL("c.workspace_id", "c.id") + `) pos ON TRUE
LEFT JOIN LATERAL (
	SELECT pls.response_time, pls.checked_at
	FROM (` + currentLatestStatisticsSQL + `) pls
	WHERE pls.proxy_id = c.id AND pls.checking_workspace_id = c.workspace_id
	ORDER BY pls.checked_at DESC, pls.statistic_id DESC
	LIMIT 1
) latest ON TRUE
ORDER BY alive DESC, latest_check DESC NULLS LAST, c.id ASC
`
	if err := DB.Raw(query, workspaceID, limit).Scan(&rows).Error; err != nil {
		return nil
	}

	result := make([]dto.ProxyRecentCheck, 0, len(rows))
	for _, row := range rows {
		latestCheck := time.Time{}
		if row.LatestCheck.Valid {
			latestCheck = row.LatestCheck.Time
		}

		result = append(result, dto.ProxyRecentCheck{
			ID:           row.ID,
			IP:           row.IPAddress,
			Port:         row.Port,
			ResponseTime: row.ResponseTime,
			Alive:        row.Alive,
			LatestCheck:  latestCheck,
		})
	}

	if version.Dirty || currentCheckerHealthVersion(workspaceID) != version {
		return []dto.ProxyRecentCheck{}
	}
	dashboardRecentChecksCache.Store(
		dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: limit},
		dashboardHealthCacheEntry[[]dto.ProxyRecentCheck]{version: version, value: result},
	)
	return result
}

func GetFastestAliveProxies(workspaceID uint, limit int) []dto.ProxyFastestAlive {
	version := currentCheckerHealthVersion(workspaceID)
	if version.Dirty {
		return []dto.ProxyFastestAlive{}
	}
	if limit <= 0 {
		return []dto.ProxyFastestAlive{}
	}

	key := dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: limit}
	if cached, ok := dashboardFastestAliveCache.Load(key); ok {
		entry := cached.(dashboardHealthCacheEntry[[]dto.ProxyFastestAlive])
		if entry.version == version {
			return entry.value
		}
	}

	return RefreshFastestAliveProxiesCache(workspaceID, limit)
}

func RefreshFastestAliveProxiesCache(workspaceID uint, limit int) []dto.ProxyFastestAlive {
	version := currentCheckerHealthVersion(workspaceID)
	if limit <= 0 {
		return []dto.ProxyFastestAlive{}
	}

	latestAliveStats := currentLatestStatistics(DB, workspaceID).
		Select("DISTINCT ON (pls.proxy_id) pls.proxy_id, pls.response_time, pls.checked_at").
		Where("pls.alive = ?", true).
		Order("pls.proxy_id, pls.response_time ASC, pls.checked_at DESC, pls.statistic_id DESC")

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
	if err := DB.Model(&domain.Proxy{}).
		Select(
			"proxies.id AS id, "+
				"proxies.host AS ip_address, "+
				"proxies.port AS port, "+
				"COALESCE(latest.response_time, 0) AS response_time, "+
				"COALESCE(NULLIF(proxies.country, ''), 'N/A') AS country, "+
				"LOWER(COALESCE(NULLIF(pr.label, ''), 'unknown')) AS reputation_label, "+
				"COALESCE(pr.score, 0) AS reputation_score, "+
				"latest.checked_at AS latest_check",
		).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.workspace_id = ? AND up.state = ?", workspaceID, domain.ManagedProxyStateActive).
		Joins("JOIN (?) AS latest ON latest.proxy_id = proxies.id", latestAliveStats).
		Joins("LEFT JOIN proxy_reputations pr ON pr.proxy_id = proxies.id AND pr.kind = ?", domain.ProxyReputationKindOverall).
		Order("latest.response_time ASC, latest.checked_at DESC, proxies.id ASC").
		Limit(limit).
		Scan(&rows).Error; err != nil {
		return nil
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

	if version.Dirty || currentCheckerHealthVersion(workspaceID) != version {
		return []dto.ProxyFastestAlive{}
	}
	dashboardFastestAliveCache.Store(
		dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: limit},
		dashboardHealthCacheEntry[[]dto.ProxyFastestAlive]{version: version, value: result},
	)
	return result
}
