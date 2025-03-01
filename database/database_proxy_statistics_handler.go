package database

import (
	"github.com/charmbracelet/log"
	"magpie/checker/redis_queue"
	"magpie/models"
	"sync/atomic"
	"time"
)

const (
	timeBetweenInsert        = time.Second * 15
	StatisticsBatchThreshold = 50000
	workerCount              = 1
)

var (
	proxyStatisticQueue = make(chan models.ProxyStatistic, 1000000)
	threadCount         = atomic.Int32{}
)

func StartProxyStatisticsRoutine() {
	go proxyStatisticsWorker()
	for {
		cnt, err := redis_queue.PublicProxyQueue.GetProxyCount()
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
	var proxyStatistics []models.ProxyStatistic
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

func AddProxyStatistic(proxyStatistic models.ProxyStatistic) {
	proxyStatisticQueue <- proxyStatistic
}

func flushStatistics(statistics *[]models.ProxyStatistic) {
	if len(*statistics) == 0 {
		return
	}

	toInsert := *statistics
	*statistics = nil

	batchSize := determineBatchSize(len(toInsert))

	go func(stats []models.ProxyStatistic) {
		if err := insertProxyStatistics(stats, batchSize); err != nil {
			log.Error("Failed to insert proxy statistics", "error", err)
		}
	}(toInsert)
}

func determineBatchSize(statCount int) int {
	numFields, err := getNumDatabaseFields(models.ProxyStatistic{}, DB)
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

func insertProxyStatistics(statistics []models.ProxyStatistic, batchSize int) error {
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
