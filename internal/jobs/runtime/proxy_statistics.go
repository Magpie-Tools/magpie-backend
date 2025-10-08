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
		defer statisticsFlushTracker.Done()

		dbCtx, cancel := context.WithTimeout(context.Background(), statisticsInsertTimeout)
		defer cancel()

		if err := database.InsertProxyStatistics(dbCtx, stats, size); err != nil {
			log.Error("Failed to insert proxy statistics", "error", err, "count", len(stats))
		}
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
