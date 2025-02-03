package database

import (
	"errors"
	"fmt"
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/checker/statistics"
	"magpie/models"
)

const (
	batchThreshold    = 30000 // Use batches when exceeding this number of records
	maxParamsPerBatch = 65535 // Conservative default (PostgreSQL's limit)
	minBatchSize      = 100   // Minimum batch size to maintain efficiency
)

func InsertAndGetProxies(proxies []models.Proxy) ([]models.Proxy, error) {
	proxyLength := len(proxies)

	if proxyLength == 0 {
		return proxies, nil
	}

	// Determine batch size
	batchSize := len(proxies)
	if proxyLength > batchThreshold {
		numFields, err := getNumDatabaseFields(models.Proxy{}, DB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse model schema: %w", err)
		}
		if numFields == 0 {
			return nil, errors.New("model has no database fields")
		}

		batchSize = maxParamsPerBatch / numFields
		if batchSize < minBatchSize {
			batchSize = minBatchSize
		}
		if batchSize > proxyLength {
			batchSize = proxyLength
		}
	}

	tx := DB.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			log.Errorf("Transaction rolled back due to panic: %v", r)
		}
	}()

	result := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		DoNothing: true,
	}).CreateInBatches(proxies, batchSize)

	if result.Error != nil {
		tx.Rollback()
		return nil, result.Error
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	// I hate that I have to do this after trying the insert
	seen := make(map[string]struct{}, len(proxies))
	uniqueProxies := make([]models.Proxy, 0, len(proxies))

	for _, p := range proxies {
		hashStr := string(p.Hash)
		if _, exists := seen[hashStr]; !exists {
			seen[hashStr] = struct{}{}
			uniqueProxies = append(uniqueProxies, p)
		}
	}

	// Collect all hashes from the proxies slice
	hashes := make([][]byte, 0, len(uniqueProxies))
	for _, p := range uniqueProxies {
		hashes = append(hashes, p.Hash)
	}

	// Query all proxies with these hashes to get their IDs
	var existingProxies []models.Proxy
	for i := 0; i < len(hashes); i += batchSize {
		end := i + batchSize
		if end > len(hashes) {
			end = len(hashes)
		}
		batch := hashes[i:end]
		var batchProxies []models.Proxy
		if err := DB.Where("hash IN ?", batch).Find(&batchProxies).Error; err != nil {
			return nil, err
		}
		existingProxies = append(existingProxies, batchProxies...)
	}

	statistics.IncreaseProxyCount(int64(len(existingProxies)))
	return existingProxies, nil
}

func getNumDatabaseFields(model interface{}, db *gorm.DB) (int, error) {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return 0, err
	}
	return len(stmt.Schema.DBNames), nil
}

func GetAllProxyCount() int64 {
	var count int64
	DB.Model(&models.Proxy{}).Count(&count)
	return count
}
