package database

import (
	"github.com/charmbracelet/log"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/checker/queue/proxy"
	"sync/atomic"
	"time"
)

const (
	timeBetweenInsert        = time.Second * 15
	StatisticsBatchThreshold = 50000
	workerCount              = 1
)

var (
	proxyStatisticQueue = make(chan domain.ProxyStatistic, 1000000)
	threadCount         = atomic.Int32{}
)

func StartProxyStatisticsRoutine() {
	go proxyStatisticsWorker()
	for {
		cnt, err := proxyqueue.PublicProxyQueue.GetProxyCount()
		if err == nil {
			if workerCount < cnt/1000000 {
				threadCount.Add(1)
				go proxyStatisticsWorker()
			}
		}

		time.Sleep(time.Minute * 5)
	}
}

func proxyStatisticsWorker() {
	defer threadCount.Add(-1)
	var proxyStatistics []domain.ProxyStatistic
	ticker := time.NewTicker(timeBetweenInsert)
	defer ticker.Stop()

	for {
		select {
		case stat := <-proxyStatisticQueue:
			proxyStatistics = append(proxyStatistics, stat)
			if len(proxyStatistics) >= StatisticsBatchThreshold {
				flushStatistics(&proxyStatistics)
			}
		case <-ticker.C:
			flushStatistics(&proxyStatistics)
		}
	}
}

func AddProxyStatistic(proxyStatistic domain.ProxyStatistic) {
	proxyStatisticQueue <- proxyStatistic
}

func flushStatistics(statistics *[]domain.ProxyStatistic) {
	if len(*statistics) == 0 {
		return
	}

	toInsert := *statistics
	*statistics = nil

	batchSize := determineBatchSize(len(toInsert))

	go func(stats []domain.ProxyStatistic) {
		if err := insertProxyStatistics(stats, batchSize); err != nil {
			log.Error("Failed to insert proxy statistics", "error", err)
		}
	}(toInsert)
}

func determineBatchSize(statCount int) int {
	numFields, err := getNumDatabaseFields(domain.ProxyStatistic{}, DB)
	if err != nil || numFields == 0 {
		log.Error("Failed to determine batch size", "error", err)
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

func insertProxyStatistics(statistics []domain.ProxyStatistic, batchSize int) error {
	tx := DB.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			log.Errorf("Transaction rolled back due to panic: %v", r)
		}
	}()

	result := tx.CreateInBatches(statistics, batchSize)
	if result.Error != nil {
		tx.Rollback()
		return result.Error
	}

	return tx.Commit().Error
}
