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
	"gorm.io/gorm/clause"
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

// InsertProxyStatistics commits accepted history, replay identities, check/usage
// counters, latest evidence and reputation-refresh requests in one transaction.
// Stream replays are filtered before any of those side effects. Nil means the
// transaction succeeded or there was nothing new to write. Filter/source read
// models are notified after commit and may still lag when this call returns.
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
		latestEntries, legacyProxyIDs := latestProxyStatusEntries(accepted)
		if err := upsertProxyLatestStatistics(tx, latestEntries); err != nil {
			return err
		}
		proxyIDs = collectProxyIDsFromStatistics(accepted)
		if err := queueProxyReputationRefresh(tx, proxyIDs); err != nil {
			return err
		}
		// Usage rows are shared by every route in a workspace and by imports.
		// Acquire them after the larger writes. Keep the legacy aggregate after
		// this barrier: its next statement must see a prior writer's newly
		// committed protocol results, including when no refresh table exists.
		if err := recordWorkspaceCheckUsage(tx, accepted); err != nil {
			return err
		}
		return upsertProxyOverallStatuses(tx, legacyProxyIDs)
	}); err != nil {
		return err
	}

	QueueReadModelRefreshForProxyIDs(proxyIDs)
	return nil
}

// acceptProxyStatisticEvents returns only newly accepted stream events and
// identity-free memory results. The caller must use the same transaction for
// acceptance and every result side effect; otherwise a failed write could make
// a replay look committed. Stream floors also reject old events after individual
// ledger rows have been retired, so pruning cannot re-enable historical replay.
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
		events = append(events, domain.ProxyStatisticEvent{Stream: key.stream, EventID: key.id, SortID: ProxyStatisticEventSortID(key.id)})
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
	streams := make([]domain.ProxyStatisticStream, 0)
	streamIDs := make([]string, 0)
	for _, event := range events {
		if len(streamIDs) == 0 || streamIDs[len(streamIDs)-1] != event.Stream {
			streams = append(streams, domain.ProxyStatisticStream{Stream: event.Stream})
			streamIDs = append(streamIDs, event.Stream)
		}
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&streams).Error; err != nil {
		return nil, err
	}

	acceptedKeys := make(map[eventKey]struct{}, len(events))
	for start := 0; start < len(events); start += 1000 {
		chunk := events[start:min(start+1000, len(events))]
		values := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)*4+1)
		for i, event := range chunk {
			values[i] = "(?, ?, ?, ?)"
			if isPostgresDialector(tx) {
				values[i] = "(?, ?, ?, ?::timestamptz)"
			}
			args = append(args, event.Stream, event.EventID, event.SortID, time.Now().UTC())
		}
		var inserted []domain.ProxyStatisticEvent
		lock := ""
		if isPostgresDialector(tx) {
			lock = " FOR SHARE"
		}
		// Lock floors inside the existing ledger insert. Retirement cannot
		// delete an identity while an ingestion transaction accepts its replay.
		query := "WITH incoming(stream, event_id, sort_id, created_at) AS (VALUES " + strings.Join(values, ",") +
			"), floors AS (SELECT stream, replay_before FROM proxy_statistic_streams WHERE stream IN ? ORDER BY stream" + lock + ") " +
			"INSERT INTO proxy_statistic_events (stream, event_id, sort_id, created_at) SELECT i.stream, i.event_id, i.sort_id, i.created_at " +
			"FROM incoming i JOIN floors f ON f.stream = i.stream WHERE i.sort_id = '' OR i.sort_id >= f.replay_before " +
			"ON CONFLICT (stream, event_id) DO NOTHING RETURNING stream, event_id, created_at"
		args = append(args, streamIDs)
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
