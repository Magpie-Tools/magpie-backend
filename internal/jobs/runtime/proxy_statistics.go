package runtime

import (
	"context"
	"sync"
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
)

const (
	statisticsFlushInterval  = 15 * time.Second
	statisticsBatchThreshold = 50000
	statisticsInsertTimeout  = 30 * time.Second
	reputationRecalcTimeout  = 10 * time.Second
)

var (
	proxyStatisticQueue    = make(chan domain.ProxyStatistic, 1_000_000)
	statisticsFlushTracker sync.WaitGroup
)

func AddProxyStatistic(proxyStatistic domain.ProxyStatistic) {
	proxyStatisticQueue <- proxyStatistic
}

func StartProxyStatisticsRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	var buffer []domain.ProxyStatistic
	timer := time.NewTimer(statisticsFlushInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			drainProxyStatisticQueue(&buffer)
			flushProxyStatistics(&buffer)
			statisticsFlushTracker.Wait()
			return
		case stat := <-proxyStatisticQueue:
			buffer = append(buffer, stat)
			if len(buffer) >= statisticsBatchThreshold {
				flushProxyStatistics(&buffer)
				resetTimer(timer)
			}
		case <-timer.C:
			flushProxyStatistics(&buffer)
			timer.Reset(statisticsFlushInterval)
		}
	}
}

func flushProxyStatistics(buffer *[]domain.ProxyStatistic) {
	if len(*buffer) == 0 {
		return
	}

	toInsert := *buffer
	*buffer = nil

	statisticsFlushTracker.Add(1)

	go func(stats []domain.ProxyStatistic) {
		start := time.Now()
		defer statisticsFlushTracker.Done()

		dbCtx, cancel := context.WithTimeout(context.Background(), statisticsInsertTimeout)
		defer cancel()

		preparedStats, proxyIDs, err := prepareProxyStatistics(dbCtx, stats)
		if err != nil {
			log.Error("Failed to prepare proxy statistics", "error", err)
			return
		}
		if len(preparedStats) == 0 {
			return
		}

		batchSize := database.CalculateProxyStatisticBatchSize(len(preparedStats))
		if err := database.InsertProxyStatistics(dbCtx, preparedStats, batchSize); err != nil {
			log.Error("Failed to insert proxy statistics", "error", err, "count", len(preparedStats))
			return
		}

		if len(proxyIDs) == 0 {
			return
		}

		repCtx, cancel := context.WithTimeout(context.Background(), reputationRecalcTimeout)
		defer cancel()

		if err := database.RecalculateProxyReputations(repCtx, proxyIDs); err != nil {
			log.Error("Failed to update proxy reputations", "error", err, "proxy_ids", proxyIDs)
		}
		log.Info("Inserted proxy statistics", "seconds", time.Since(start).Seconds())
	}(toInsert)
}

func drainProxyStatisticQueue(buffer *[]domain.ProxyStatistic) {
	for {
		select {
		case stat := <-proxyStatisticQueue:
			*buffer = append(*buffer, stat)
		default:
			return
		}
	}
}

func resetTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(statisticsFlushInterval)
}

func collectProxyIDs(stats []domain.ProxyStatistic) []uint64 {
	if len(stats) == 0 {
		return nil
	}

	seen := make(map[uint64]struct{}, len(stats))
	for _, stat := range stats {
		if stat.ProxyID == 0 {
			continue
		}
		seen[stat.ProxyID] = struct{}{}
	}

	if len(seen) == 0 {
		return nil
	}

	ids := make([]uint64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}

	return ids
}

func prepareProxyStatistics(ctx context.Context, stats []domain.ProxyStatistic) ([]domain.ProxyStatistic, []uint64, error) {
	if len(stats) == 0 {
		return nil, nil, nil
	}

	proxyIDs := collectProxyIDs(stats)
	if len(proxyIDs) == 0 {
		return stats, nil, nil
	}

	existing, err := database.GetExistingProxyIDSet(ctx, proxyIDs)
	if err != nil {
		return nil, nil, err
	}

	if len(existing) == 0 {
		return nil, nil, nil
	}

	if len(existing) == len(proxyIDs) {
		return stats, proxyIDs, nil
	}

	filtered := make([]domain.ProxyStatistic, 0, len(stats))
	for _, stat := range stats {
		if _, ok := existing[stat.ProxyID]; ok {
			filtered = append(filtered, stat)
		}
	}

	if len(filtered) == 0 {
		return nil, nil, nil
	}

	validIDs := make([]uint64, 0, len(existing))
	for id := range existing {
		validIDs = append(validIDs, id)
	}

	dropped := len(stats) - len(filtered)
	if dropped > 0 {
		log.Info("Skipped proxy statistics for removed proxies", "dropped", dropped)
	}

	return filtered, validIDs, nil
}
