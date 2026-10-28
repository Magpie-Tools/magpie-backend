package database

import (
	"context"
	"fmt"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

const (
	defaultProxySnapshotLimit = 96
	maxProxySnapshotLimit     = 720
)

// SaveProxySnapshots stores snapshots for active routes in every workspace.
func SaveProxySnapshots(ctx context.Context) error {
	if DB == nil {
		return fmt.Errorf("database: connection was not configured")
	}

	tx := DB
	if ctx != nil {
		tx = tx.WithContext(ctx)
	}

	var workspaceIDs []uint
	if err := tx.Model(&domain.Workspace{}).Pluck("id", &workspaceIDs).Error; err != nil {
		return fmt.Errorf("proxy snapshot: fetch workspace ids: %w", err)
	}

	if len(workspaceIDs) == 0 {
		return nil
	}

	aliveCountByWorkspace, err := aliveProxyCountByWorkspace(tx, workspaceIDs)
	if err != nil {
		return err
	}

	scrapedCountByWorkspace, err := scrapedProxyCountByWorkspace(tx, workspaceIDs)
	if err != nil {
		return err
	}

	snapshots := make([]domain.ProxySnapshot, 0, len(workspaceIDs)*2)
	for _, workspaceID := range workspaceIDs {
		snapshots = append(snapshots,
			domain.ProxySnapshot{
				WorkspaceID: workspaceID,
				Metric:      domain.ProxySnapshotMetricAlive,
				Count:       aliveCountByWorkspace[workspaceID],
			},
			domain.ProxySnapshot{
				WorkspaceID: workspaceID,
				Metric:      domain.ProxySnapshotMetricScraped,
				Count:       scrapedCountByWorkspace[workspaceID],
			},
		)
	}

	if len(snapshots) == 0 {
		return nil
	}

	if err := tx.Create(&snapshots).Error; err != nil {
		return fmt.Errorf("proxy snapshot: insert rows: %w", err)
	}

	return nil
}

// GetProxySnapshotEntries returns the most recent snapshots for a workspace and metric.
func GetProxySnapshotEntries(workspaceID uint, metric string, limit int) []dto.ProxySnapshotEntry {
	if DB == nil {
		return nil
	}

	if metric != domain.ProxySnapshotMetricAlive && metric != domain.ProxySnapshotMetricScraped {
		return nil
	}

	limit = normalizeProxySnapshotLimit(limit)

	rows := make([]domain.ProxySnapshot, 0, limit)

	DB.Where("workspace_id = ? AND metric = ?", workspaceID, metric).
		Order("created_at DESC").
		Limit(limit).
		Find(&rows)

	if len(rows) == 0 {
		return nil
	}

	entries := make([]dto.ProxySnapshotEntry, len(rows))
	for index := range rows {
		row := rows[len(rows)-1-index]
		entries[index] = dto.ProxySnapshotEntry{
			Count:      row.Count,
			RecordedAt: row.CreatedAt,
		}
	}

	return entries
}

func normalizeProxySnapshotLimit(limit int) int {
	if limit <= 0 {
		return defaultProxySnapshotLimit
	}
	if limit > maxProxySnapshotLimit {
		return maxProxySnapshotLimit
	}
	return limit
}

func aliveProxyCountByWorkspace(tx *gorm.DB, workspaceIDs []uint) (map[uint]int64, error) {
	counts := make(map[uint]int64, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		counts[workspaceID] = 0
	}
	if len(workspaceIDs) == 0 {
		return counts, nil
	}

	var rows []struct {
		WorkspaceID uint
		AliveCount  int64
	}

	activeOwnership := `SELECT workspace_id, proxy_id FROM user_proxies
WHERE workspace_id IN ? AND state = ?`
	if isPostgresDialect(tx) {
		// Keep the requested ownership scope as one input to the join. Dense
		// current evidence can otherwise cause an underestimated nested loop
		// to probe the ownership primary key once for every alive route.
		activeOwnership += ` OFFSET 0`
	}

	// Count matching evidence before joining ownership. Correlating an overall
	// status aggregate to every owned route repeats the evidence lookup millions
	// of times, even when most routes have no current alive result.
	if err := tx.Raw(`SELECT alive.workspace_id, COUNT(*) AS alive_count
FROM (
    SELECT DISTINCT pls.workspace_id, pls.proxy_id
    FROM workspaces w
    JOIN proxy_latest_statistics pls ON pls.workspace_id = w.id AND pls.alive = TRUE
    LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = w.id AND cp.proxy_id = pls.proxy_id AND cp.protocol_id = pls.protocol_id
    WHERE w.id IN ? AND w.checker_generation > 0 AND NOT w.checker_dirty
      AND pls.config_key <> '' AND pls.config_key = COALESCE(cp.config_key,
          CASE pls.protocol_id WHEN 1 THEN w.checker_http_key WHEN 2 THEN w.checker_https_key
          WHEN 3 THEN w.checker_socks4_key WHEN 4 THEN w.checker_socks5_key ELSE '' END)
) alive
JOIN (`+activeOwnership+`) up ON up.workspace_id = alive.workspace_id AND up.proxy_id = alive.proxy_id
GROUP BY alive.workspace_id
UNION ALL
SELECT up.workspace_id, COUNT(*) AS alive_count
FROM workspaces w
JOIN user_proxies up ON up.workspace_id = w.id AND up.state = ?
JOIN proxy_overall_statuses pos ON pos.proxy_id = up.proxy_id AND pos.overall_alive = TRUE
WHERE w.id IN ? AND w.checker_generation = 0 AND NOT w.checker_dirty
GROUP BY up.workspace_id`, workspaceIDs, workspaceIDs, domain.ManagedProxyStateActive,
		domain.ManagedProxyStateActive, workspaceIDs).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("proxy snapshot: aggregate alive counts: %w", err)
	}

	for _, row := range rows {
		counts[row.WorkspaceID] = row.AliveCount
	}

	return counts, nil
}

func scrapedProxyCountByWorkspace(tx *gorm.DB, workspaceIDs []uint) (map[uint]int64, error) {
	var rows []struct {
		WorkspaceID  uint
		ScrapedCount int64
	}

	if err := tx.Table("user_proxies AS up").
		Select("up.workspace_id AS workspace_id, COUNT(*) AS scraped_count").
		Where("up.workspace_id IN ? AND up.state = ?", workspaceIDs, domain.ManagedProxyStateActive).
		Where("EXISTS (SELECT 1 FROM proxy_scrape_site ps WHERE ps.proxy_id = up.proxy_id)").
		Group("up.workspace_id").
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("proxy snapshot: aggregate scraped counts: %w", err)
	}

	counts := make(map[uint]int64, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		counts[workspaceID] = 0
	}

	for _, row := range rows {
		counts[row.WorkspaceID] = row.ScrapedCount
	}

	return counts, nil
}

// GetCurrentAliveProxyCount returns the workspace's latest alive proxy count based on proxy statistics.
func GetCurrentAliveProxyCount(workspaceID uint) int64 {
	if DB == nil {
		return 0
	}

	counts, err := aliveProxyCountByWorkspace(DB, []uint{workspaceID})
	if err != nil {
		return 0
	}

	return counts[workspaceID]
}

func GetCurrentScrapedProxyCount(workspaceID uint) int64 {
	if DB == nil {
		return 0
	}

	counts, err := scrapedProxyCountByWorkspace(DB, []uint{workspaceID})
	if err != nil {
		return 0
	}

	return counts[workspaceID]
}

type proxySnapshotCountSummary struct {
	Current  int64
	Increase int64
	Found    bool
}

func getProxySnapshotCountSummary(workspaceID uint, metric string, since time.Time) proxySnapshotCountSummary {
	summary, _ := getProxySnapshotCountSummaryWithDB(DB, workspaceID, metric, since)
	return summary
}

func getProxySnapshotCountSummaryWithDB(db *gorm.DB, workspaceID uint, metric string, since time.Time) (proxySnapshotCountSummary, error) {
	if db == nil || workspaceID == 0 {
		return proxySnapshotCountSummary{}, nil
	}

	var latest domain.ProxySnapshot
	latestResult := db.
		Where("workspace_id = ? AND metric = ?", workspaceID, metric).
		Order("created_at DESC, id DESC").
		Limit(1).
		Find(&latest)
	if latestResult.Error != nil || latestResult.RowsAffected == 0 {
		return proxySnapshotCountSummary{}, latestResult.Error
	}

	var baseline domain.ProxySnapshot
	baselineResult := db.
		Where("workspace_id = ? AND metric = ? AND created_at >= ?", workspaceID, metric, since).
		Order("created_at ASC, id ASC").
		Limit(1).
		Find(&baseline)

	increase := int64(0)
	if baselineResult.Error == nil && baselineResult.RowsAffected > 0 && latest.Count > baseline.Count {
		increase = latest.Count - baseline.Count
	}

	return proxySnapshotCountSummary{
		Current:  latest.Count,
		Increase: increase,
		Found:    true,
	}, baselineResult.Error
}
