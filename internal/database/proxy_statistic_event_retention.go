package database

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Decimal padding preserves Redis's numeric ID ordering in both databases.
func ProxyStatisticEventSortID(id string) string {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return ""
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return ""
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%020d-%020d", ms, seq)
}

// RetireProxyStatisticEvents persists the replay floor before removing a
// bounded batch. Callers must derive before from acknowledged Redis progress,
// never from the age of an event or the largest ID committed by one worker.
func RetireProxyStatisticEvents(ctx context.Context, stream, before string, limit int) (int64, error) {
	if DB == nil {
		return 0, nil
	}
	floor := ProxyStatisticEventSortID(before)
	if floor == "" {
		return 0, fmt.Errorf("invalid statistics replay floor %q", before)
	}
	var deleted int64
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := domain.ProxyStatisticStream{Stream: stream, ReplayBefore: floor}
		maximum := "MAX(proxy_statistic_streams.replay_before, EXCLUDED.replay_before)"
		if isPostgresDialector(tx) {
			maximum = "GREATEST(proxy_statistic_streams.replay_before, EXCLUDED.replay_before)"
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "stream"}}, DoUpdates: clause.Assignments(map[string]any{"replay_before": gorm.Expr(maximum), "updated_at": gorm.Expr("CURRENT_TIMESTAMP")})}).Create(&row).Error; err != nil {
			return err
		}
		// Old installations have no sortable key. Normalize only a bounded
		// batch per pass so upgrade does not rewrite the lifetime ledger at once.
		if isPostgresDialector(tx) {
			query := `UPDATE proxy_statistic_events SET sort_id = LPAD(split_part(event_id, '-', 1), 20, '0') || '-' || LPAD(split_part(event_id, '-', 2), 20, '0')
			WHERE (stream, event_id) IN (SELECT stream, event_id FROM proxy_statistic_events WHERE stream = ? AND sort_id = '' AND event_id ~ '^[0-9]{1,20}-[0-9]{1,20}$' LIMIT ?)`
			if err := tx.Exec(query, stream, max(1, limit)).Error; err != nil {
				return err
			}
		} else {
			var legacy []domain.ProxyStatisticEvent
			if err := tx.Where("stream = ? AND sort_id = ''", stream).Limit(max(1, limit)).Find(&legacy).Error; err != nil {
				return err
			}
			for _, event := range legacy {
				if key := ProxyStatisticEventSortID(event.EventID); key != "" {
					if err := tx.Model(&domain.ProxyStatisticEvent{}).Where("stream = ? AND event_id = ?", stream, event.EventID).Update("sort_id", key).Error; err != nil {
						return err
					}
				}
			}
		}
		query := `DELETE FROM proxy_statistic_events WHERE stream = ? AND event_id IN (SELECT event_id FROM proxy_statistic_events WHERE stream = ? AND sort_id <> '' AND sort_id < (SELECT replay_before FROM proxy_statistic_streams WHERE stream = ?) ORDER BY sort_id LIMIT ?)`
		result := tx.Exec(query, stream, stream, stream, max(1, limit))
		deleted = result.RowsAffected
		return result.Error
	})
	return deleted, err
}
