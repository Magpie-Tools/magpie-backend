package database

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
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
	value, _ := GetRecentProxyChecksContext(context.Background(), workspaceID, limit)
	return value
}

func GetRecentProxyChecksContext(ctx context.Context, workspaceID uint, limit int) ([]dto.ProxyRecentCheck, error) {
	if limit <= 0 {
		limit = defaultRecentProxyChecksLimit
	} else if limit > maxRecentProxyChecksLimit {
		limit = maxRecentProxyChecksLimit
	}

	value, err := getDashboardComponent(ctx, dashboardCacheKey{workspaceID: workspaceID, component: dashboardRecentComponent, limit: limit})
	if err != nil {
		return nil, err
	}
	return value.([]dto.ProxyRecentCheck), nil
}

func RefreshRecentProxyChecksCache(workspaceID uint, limit int) []dto.ProxyRecentCheck {
	if limit <= 0 {
		limit = defaultRecentProxyChecksLimit
	} else if limit > maxRecentProxyChecksLimit {
		limit = maxRecentProxyChecksLimit
	}
	value, err := refreshDashboardComponent(context.Background(), dashboardCacheKey{workspaceID: workspaceID, component: dashboardRecentComponent, limit: limit})
	if err != nil {
		return nil
	}
	return value.([]dto.ProxyRecentCheck)
}

func refreshRecentProxyChecksCache(db *gorm.DB, workspaceID uint, limit int) ([]dto.ProxyRecentCheck, error) {
	if db == nil {
		return nil, fmt.Errorf("dashboard recent checks: database connection was not initialised")
	}
	version, err := currentCheckerHealthVersionWithDB(db, workspaceID)
	if err != nil {
		return nil, err
	}
	if version.Dirty {
		return []dto.ProxyRecentCheck{}, nil
	}
	if limit <= 0 {
		limit = defaultRecentProxyChecksLimit
	} else if limit > maxRecentProxyChecksLimit {
		limit = maxRecentProxyChecksLimit
	}
	key := dashboardCacheKey{workspaceID: workspaceID, component: dashboardRecentComponent, limit: limit}
	revision := dashboardCaches.revision(key)

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
	if err := db.Raw(query, workspaceID, limit).Scan(&rows).Error; err != nil {
		return nil, err
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

	currentVersion, err := currentCheckerHealthVersionWithDB(db, workspaceID)
	if err != nil {
		return nil, err
	}
	if currentVersion != version {
		return []dto.ProxyRecentCheck{}, nil
	}
	dashboardCaches.publish(key, revision, func() {
		dashboardRecentChecksCache.Store(
			dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: limit},
			dashboardHealthCacheEntry[[]dto.ProxyRecentCheck]{version: version, value: result, completedAt: dashboardCaches.now()},
		)
	})
	return result, nil
}

func GetFastestAliveProxies(workspaceID uint, limit int) []dto.ProxyFastestAlive {
	value, _ := GetFastestAliveProxiesContext(context.Background(), workspaceID, limit)
	return value
}

func GetFastestAliveProxiesContext(ctx context.Context, workspaceID uint, limit int) ([]dto.ProxyFastestAlive, error) {
	if limit <= 0 {
		return []dto.ProxyFastestAlive{}, nil
	}
	value, err := getDashboardComponent(ctx, dashboardCacheKey{workspaceID: workspaceID, component: dashboardFastestComponent, limit: limit})
	if err != nil {
		return nil, err
	}
	return value.([]dto.ProxyFastestAlive), nil
}

func RefreshFastestAliveProxiesCache(workspaceID uint, limit int) []dto.ProxyFastestAlive {
	value, err := refreshDashboardComponent(context.Background(), dashboardCacheKey{workspaceID: workspaceID, component: dashboardFastestComponent, limit: limit})
	if err != nil {
		return nil
	}
	return value.([]dto.ProxyFastestAlive)
}

func refreshFastestAliveProxiesCache(db *gorm.DB, workspaceID uint, limit int) ([]dto.ProxyFastestAlive, error) {
	if db == nil {
		return nil, fmt.Errorf("dashboard fastest proxies: database connection was not initialised")
	}
	version, err := currentCheckerHealthVersionWithDB(db, workspaceID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []dto.ProxyFastestAlive{}, nil
	}
	if version.Dirty {
		return []dto.ProxyFastestAlive{}, nil
	}
	key := dashboardCacheKey{workspaceID: workspaceID, component: dashboardFastestComponent, limit: limit}
	revision := dashboardCaches.revision(key)

	latestAliveStats := dashboardLatestStatistics(db, workspaceID).
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

	currentVersion, err := currentCheckerHealthVersionWithDB(db, workspaceID)
	if err != nil {
		return nil, err
	}
	if currentVersion != version {
		return []dto.ProxyFastestAlive{}, nil
	}
	dashboardCaches.publish(key, revision, func() {
		dashboardFastestAliveCache.Store(
			dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: limit},
			dashboardHealthCacheEntry[[]dto.ProxyFastestAlive]{version: version, value: result, completedAt: dashboardCaches.now()},
		)
	})
	return result, nil
}
