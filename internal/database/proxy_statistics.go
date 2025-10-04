package database

import (
	"github.com/charmbracelet/log"
	"magpie/internal/domain"
)

func CalculateProxyStatisticBatchSize(statCount int) int {
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

func InsertProxyStatistics(statistics []domain.ProxyStatistic, batchSize int) error {
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
