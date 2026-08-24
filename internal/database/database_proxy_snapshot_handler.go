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

// GetProxySnapshotEntries returns the most recent proxy snapshot entries for a user/metric combination.
func GetProxySnapshotEntries(userID uint, metric string, limit int) []dto.ProxySnapshotEntry {
	if DB == nil {
		return nil
	}

	if metric != domain.ProxySnapshotMetricAlive && metric != domain.ProxySnapshotMetricScraped {
		return nil
	}

	limit = normalizeProxySnapshotLimit(limit)

	rows := make([]domain.ProxySnapshot, 0, limit)

	DB.Where("workspace_id = ? AND metric = ?", userID, metric).
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
	var rows []struct {
		WorkspaceID uint
		AliveCount  int64
	}

	if err := tx.Table("user_proxies AS up").
		Select("up.workspace_id AS workspace_id, COUNT(DISTINCT up.proxy_id) AS alive_count").
		Joins("JOIN proxy_overall_statuses pos ON pos.proxy_id = up.proxy_id").
		Where("up.workspace_id IN ? AND up.state = ?", workspaceIDs, domain.ManagedProxyStateActive).
		Where("pos.overall_alive = ?", true).
		Group("up.workspace_id").
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("proxy snapshot: aggregate alive counts: %w", err)
	}

	counts := make(map[uint]int64, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		counts[workspaceID] = 0
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

// GetCurrentAliveProxyCount returns the latest alive proxy count for a user based on proxy statistics.
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

func getProxySnapshotCountSummary(userID uint, metric string, since time.Time) proxySnapshotCountSummary {
	if DB == nil || userID == 0 {
		return proxySnapshotCountSummary{}
	}

	var latest domain.ProxySnapshot
	latestResult := DB.
		Where("workspace_id = ? AND metric = ?", userID, metric).
		Order("created_at DESC, id DESC").
		Limit(1).
		Find(&latest)
	if latestResult.Error != nil || latestResult.RowsAffected == 0 {
		return proxySnapshotCountSummary{}
	}

	var baseline domain.ProxySnapshot
	baselineResult := DB.
		Where("workspace_id = ? AND metric = ? AND created_at >= ?", userID, metric, since).
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
	}
}
