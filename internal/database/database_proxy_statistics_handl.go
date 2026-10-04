package database

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

var proxyStatisticFieldCount atomic.Int32

func CalculateProxyStatisticBatchSize(statCount int) int {
	if statCount <= 0 {
		return 0
	}

	numFields := proxyStatisticColumnCount()
	if numFields <= 0 {
		return minBatchSize
	}

	maxPossibleBatchSize := maxParamsPerBatch / numFields
	if maxPossibleBatchSize < 1 {
		maxPossibleBatchSize = 1
	}

	batchSize := maxPossibleBatchSize
	if batchSize < minBatchSize {
		batchSize = minBatchSize
	}
	if batchSize > statCount {
		batchSize = statCount
	}

	return batchSize
}

func proxyStatisticColumnCount() int {
	if count := proxyStatisticFieldCount.Load(); count > 0 {
		return int(count)
	}

	if DB == nil {
		log.Error("Failed to determine proxy statistics batch size: database not initialised")
		return 0
	}

	numFields, err := getNumDatabaseFields(domain.ProxyStatistic{}, DB)
	if err != nil || numFields == 0 {
		log.Error("Failed to determine proxy statistics batch size", "error", err)
		return 0
	}

	proxyStatisticFieldCount.Store(int32(numFields))
	return numFields
}

func InsertProxyStatistics(ctx context.Context, statistics []domain.ProxyStatistic, batchSize int) error {
	if len(statistics) == 0 {
		return nil
	}

	if DB == nil {
		return fmt.Errorf("proxy statistics: database connection was not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var proxyIDs []uint64
	if err := db.Transaction(func(tx *gorm.DB) error {
		accepted, err := acceptProxyStatisticEvents(tx, statistics)
		if err != nil || len(accepted) == 0 {
			return err
		}
		if batchSize <= 0 {
			batchSize = CalculateProxyStatisticBatchSize(len(accepted))
		}
		if err := tx.CreateInBatches(&accepted, batchSize).Error; err != nil {
			return err
		}
		if err := incrementProxyDailyChecks(tx, accepted); err != nil {
			return err
		}
		if err := recordWorkspaceCheckUsage(tx, accepted); err != nil {
			return err
		}
		if err := updateProxyStatusCaches(tx, accepted); err != nil {
			return err
		}
		proxyIDs = collectProxyIDsFromStatistics(accepted)
		return queueProxyReputationRefresh(tx, proxyIDs)
	}); err != nil {
		return err
	}

	QueueReadModelRefreshForProxyIDs(proxyIDs)
	return nil
}

func acceptProxyStatisticEvents(tx *gorm.DB, statistics []domain.ProxyStatistic) ([]domain.ProxyStatistic, error) {
	type eventKey struct{ stream, id string }
	events := make([]domain.ProxyStatisticEvent, 0, len(statistics))
	seen := make(map[eventKey]struct{}, len(statistics))
	for _, stat := range statistics {
		if stat.EventID == "" {
			continue // The in-memory fallback has no replayable stream identity.
		}
		key := eventKey{stat.EventStream, stat.EventID}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		events = append(events, domain.ProxyStatisticEvent{Stream: key.stream, EventID: key.id})
	}
	if len(events) == 0 {
		return statistics, nil
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Stream != events[j].Stream {
			return events[i].Stream < events[j].Stream
		}
		return events[i].EventID < events[j].EventID
	})

	acceptedKeys := make(map[eventKey]struct{}, len(events))
	for start := 0; start < len(events); start += 1000 {
		chunk := events[start:min(start+1000, len(events))]
		values := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)*3)
		for i, event := range chunk {
			values[i] = "(?, ?, ?)"
			args = append(args, event.Stream, event.EventID, time.Now().UTC())
		}
		var inserted []domain.ProxyStatisticEvent
		query := "INSERT INTO proxy_statistic_events (stream, event_id, created_at) VALUES " + strings.Join(values, ",") +
			" ON CONFLICT (stream, event_id) DO NOTHING RETURNING stream, event_id, created_at"
		if err := tx.Raw(query, args...).Scan(&inserted).Error; err != nil {
			return nil, err
		}
		for _, event := range inserted {
			acceptedKeys[eventKey{event.Stream, event.EventID}] = struct{}{}
		}
	}
	accepted := make([]domain.ProxyStatistic, 0, len(statistics))
	for _, stat := range statistics {
		key := eventKey{stat.EventStream, stat.EventID}
		if stat.EventID != "" {
			if _, exists := acceptedKeys[key]; !exists {
				continue
			}
			delete(acceptedKeys, key)
		}
		accepted = append(accepted, stat)
	}
	return accepted, nil
}

func collectProxyIDsFromStatistics(statistics []domain.ProxyStatistic) []uint64 {
	if len(statistics) == 0 {
		return nil
	}

	seen := make(map[uint64]struct{}, len(statistics))
	proxyIDs := make([]uint64, 0, len(statistics))
	for _, stat := range statistics {
		if stat.ProxyID == 0 {
			continue
		}
		if _, exists := seen[stat.ProxyID]; exists {
			continue
		}
		seen[stat.ProxyID] = struct{}{}
		proxyIDs = append(proxyIDs, stat.ProxyID)
	}
	return proxyIDs
}
