package database

import (
	"context"
	"fmt"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

const (
	defaultProxyHistoryLimit = 24
	maxProxyHistoryLimit     = 720
)

// SaveProxyHistorySnapshot stores the active-route count for every workspace.
// It records zero counts so capacity changes remain visible over time.
func SaveProxyHistorySnapshot(ctx context.Context) error {
	if DB == nil {
		return fmt.Errorf("database: connection was not configured")
	}

	tx := DB
	if ctx != nil {
		tx = tx.WithContext(ctx)
	}

	var workspaceIDs []uint
	if err := tx.Model(&domain.Workspace{}).Pluck("id", &workspaceIDs).Error; err != nil {
		return fmt.Errorf("proxy history: fetch workspace ids: %w", err)
	}

	if len(workspaceIDs) == 0 {
		return nil
	}

	var counts []struct {
		WorkspaceID uint
		ProxyCount  int64
	}

	if err := tx.Table("user_proxies").
		Select("workspace_id, COUNT(*) AS proxy_count").
		Where("workspace_id IN ? AND state = ?", workspaceIDs, domain.ManagedProxyStateActive).
		Group("workspace_id").
		Scan(&counts).Error; err != nil {
		return fmt.Errorf("proxy history: aggregate proxy counts: %w", err)
	}

	countByWorkspace := make(map[uint]int64, len(counts))
	for _, row := range counts {
		countByWorkspace[row.WorkspaceID] = row.ProxyCount
	}

	histories := make([]domain.ProxyHistory, 0, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		histories = append(histories, domain.ProxyHistory{
			WorkspaceID: workspaceID,
			ProxyCount:  countByWorkspace[workspaceID],
		})
	}

	if len(histories) == 0 {
		return nil
	}

	if err := tx.Create(&histories).Error; err != nil {
		return fmt.Errorf("proxy history: insert rows: %w", err)
	}

	return nil
}

// GetProxyHistoryEntries returns recent snapshots for a workspace.
func GetProxyHistoryEntries(workspaceID uint, limit int) []dto.ProxyHistoryEntry {
	if DB == nil {
		return nil
	}

	limit = normalizeProxyHistoryLimit(limit)

	rows := make([]domain.ProxyHistory, 0, limit)

	DB.Where("workspace_id = ?", workspaceID).
		Order("created_at DESC").
		Limit(limit).
		Find(&rows)

	if len(rows) == 0 {
		return nil
	}

	entries := make([]dto.ProxyHistoryEntry, len(rows))
	for index := range rows {
		row := rows[len(rows)-1-index]
		entries[index] = dto.ProxyHistoryEntry{
			Count:      row.ProxyCount,
			RecordedAt: row.CreatedAt,
		}
	}

	return entries
}

func normalizeProxyHistoryLimit(limit int) int {
	if limit <= 0 {
		return defaultProxyHistoryLimit
	}
	if limit > maxProxyHistoryLimit {
		return maxProxyHistoryLimit
	}
	return limit
}
