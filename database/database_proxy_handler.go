package database

import (
	"errors"
	"fmt"
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/checker/statistics"
	"magpie/models"
	"magpie/models/routeModels"
)

const (
	batchThreshold    = 30000 // Use batches when exceeding this number of records
	maxParamsPerBatch = 65535 // Conservative default (PostgreSQL's limit)
	minBatchSize      = 100   // Minimum batch size to maintain efficiency

	proxiesPerPage = 40
)

func InsertAndGetProxies(proxies []models.Proxy, userID uint) ([]models.Proxy, error) {
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

	//TODO in db transaction?

	var associations []models.UserProxy
	for _, p := range existingProxies {
		associations = append(associations, models.UserProxy{
			UserID:  userID,
			ProxyID: p.ID,
		})
	}

	if err := DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "proxy_id"}},
		DoNothing: true,
	}).CreateInBatches(associations, batchSize).Error; err != nil {
		return nil, err
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

func GetAllProxyCountOfUser(userId uint) int64 {
	var count int64
	DB.Model(&models.Proxy{}).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.user_id = ?", userId).
		Count(&count)
	return count
}

func GetAllProxies() []models.Proxy {
	var proxies []models.Proxy
	DB.Model(&models.Proxy{}).Find(&proxies)
	return proxies
}

func GetProxyPage(userId uint, page int) []routeModels.ProxyInfo {
	offset := (page - 1) * proxiesPerPage

	subQuery := DB.Model(&models.ProxyStatistic{}).
		Select("DISTINCT ON (proxy_id) *").
		Order("proxy_id, created_at DESC")

	var results []routeModels.ProxyInfo

	DB.Model(&models.Proxy{}).
		Select(
			"CONCAT(proxies.ip1, '.', proxies.ip2, '.', proxies.ip3, '.', proxies.ip4) AS ip, "+
				"COALESCE(ps.estimated_type, 'N/A') AS estimated_type, "+
				"COALESCE(ps.response_time, 0) AS response_time, "+
				"COALESCE(ps.country, 'N/A') AS country, "+
				"COALESCE(al.name, 'N/A') AS anonymity_level, "+
				"COALESCE(pr.name, 'N/A') AS protocol, "+
				"COALESCE(ps.alive, false) AS alive, "+ // Add alive status
				"COALESCE(ps.created_at, '0001-01-01 00:00:00'::timestamp) AS latest_check",
		).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.user_id = ?", userId).
		Joins("LEFT JOIN (?) AS ps ON ps.proxy_id = proxies.id", subQuery).
		Joins("LEFT JOIN anonymity_levels al ON al.id = ps.level_id").
		Joins("LEFT JOIN protocols pr ON pr.id = ps.protocol_id").
		Order("proxies.id ASC").
		Offset(offset).
		Limit(proxiesPerPage).
		Scan(&results)

	return results
}
