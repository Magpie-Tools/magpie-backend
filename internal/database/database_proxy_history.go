package database

import (
	"context"
	"fmt"

	"magpie/internal/domain"
)

// SaveProxyHistorySnapshot stores a snapshot of the proxy count for every user at the time of invocation.
// It records zero counts as well so we can track when users have no proxies.
func SaveProxyHistorySnapshot(ctx context.Context) error {
	if DB == nil {
		return fmt.Errorf("database: connection was not configured")
	}

	tx := DB
	if ctx != nil {
		tx = tx.WithContext(ctx)
	}

	var userIDs []uint
	if err := tx.Model(&domain.User{}).Pluck("id", &userIDs).Error; err != nil {
		return fmt.Errorf("proxy history: fetch user ids: %w", err)
	}

	if len(userIDs) == 0 {
		return nil
	}

	var counts []struct {
		UserID     uint
		ProxyCount int64
	}

	if err := tx.Table("user_proxies").
		Select("user_id, COUNT(*) AS proxy_count").
		Where("user_id IN ?", userIDs).
		Group("user_id").
		Scan(&counts).Error; err != nil {
		return fmt.Errorf("proxy history: aggregate proxy counts: %w", err)
	}

	countByUser := make(map[uint]int64, len(counts))
	for _, row := range counts {
		countByUser[row.UserID] = row.ProxyCount
	}

	histories := make([]domain.ProxyHistory, 0, len(userIDs))
	for _, userID := range userIDs {
		histories = append(histories, domain.ProxyHistory{
			UserID:     userID,
			ProxyCount: countByUser[userID],
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
