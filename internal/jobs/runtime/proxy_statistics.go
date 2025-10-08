package runtime

import (
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
)

const (
	statisticsFlushInterval  = 15 * time.Second
	statisticsBatchThreshold = 50000
)

var proxyStatisticQueue = make(chan domain.ProxyStatistic, 1_000_000)

func StartProxyStatisticsRoutine() {
	go proxyStatisticsWorker()
}

func AddProxyStatistic(proxyStatistic domain.ProxyStatistic) {
	proxyStatisticQueue <- proxyStatistic
}

func proxyStatisticsWorker() {
	var buffer []domain.ProxyStatistic
	ticker := time.NewTicker(statisticsFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case stat := <-proxyStatisticQueue:
			buffer = append(buffer, stat)
			if len(buffer) >= statisticsBatchThreshold {
				flushProxyStatistics(&buffer)
			}
		case <-ticker.C:
			flushProxyStatistics(&buffer)
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

	go func(stats []domain.ProxyStatistic, size int) {
		if err := database.InsertProxyStatistics(stats, size); err != nil {
			log.Error("Failed to insert proxy statistics", "error", err)
		}
	}(toInsert, batchSize)
}
