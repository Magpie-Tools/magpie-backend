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

	batchSize := database.CalculateProxyStatisticBatchSize(len(toInsert))
	statisticsFlushTracker.Add(1)

	go func(stats []domain.ProxyStatistic, size int) {
		start := time.Now()
		defer statisticsFlushTracker.Done()

		dbCtx, cancel := context.WithTimeout(context.Background(), statisticsInsertTimeout)
		defer cancel()

		if err := database.InsertProxyStatistics(dbCtx, stats, size); err != nil {
			log.Error("Failed to insert proxy statistics", "error", err, "count", len(stats))
			return
		}

		proxyIDs := collectProxyIDs(stats)
		if len(proxyIDs) == 0 {
			return
		}

		repCtx, cancel := context.WithTimeout(context.Background(), reputationRecalcTimeout)
		defer cancel()

		if err := database.RecalculateProxyReputations(repCtx, proxyIDs); err != nil {
			log.Error("Failed to update proxy reputations", "error", err, "proxy_ids", proxyIDs)
		}
		log.Info("Inserted proxy statistics", "seconds", time.Since(start).Seconds())
	}(toInsert, batchSize)
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
