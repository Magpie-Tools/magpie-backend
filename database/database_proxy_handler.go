package database

import (
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/checker/statistics"
	"magpie/models"
	"magpie/models/routeModels"
)

const (
	batchThreshold    = 8191  // Use batches when exceeding this number of records
	maxParamsPerBatch = 65534 // Conservative default (PostgreSQL's limit) - 1
	minBatchSize      = 100   // Minimum batch size to maintain efficiency

	proxiesPerPage = 40
)

func InsertAndGetProxies(proxies []models.Proxy, userID uint) ([]models.Proxy, error) {
	// Deduplicate proxies upfront to reduce processing
	uniqueProxies := deduplicateProxies(proxies)
	if len(uniqueProxies) == 0 {
		return nil, nil
	}

	// Calculate batch size based on deduplicated count
	batchSize := calculateBatchSize(len(uniqueProxies))

	// Single transaction for all database operations
	tx := DB.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer transactionRollbackHandler(tx)

	// Bulk insert deduplicated proxies
	if err := insertProxies(tx, uniqueProxies, batchSize); err != nil {
		return nil, err
	}

	// Get existing proxies (including pre-existing ones)
	existingProxies, err := fetchExistingProxies(tx, uniqueProxies, batchSize)
	if err != nil {
		return nil, err
	}

	// Create user-proxy associations
	if err = createUserAssociations(tx, existingProxies, userID, batchSize); err != nil {
		return nil, err
	}

	// Retrieve final results with user relationships
	proxiesWithUsers, err := fetchProxiesWithUsers(tx, existingProxies)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit().Error; err != nil {
		return nil, err
	}

	statistics.IncreaseProxyCount(int64(len(proxiesWithUsers)))
	return proxiesWithUsers, nil
}

// Helper functions
func deduplicateProxies(proxies []models.Proxy) []models.Proxy {
	seen := make(map[string]struct{}, len(proxies))
	unique := make([]models.Proxy, 0, len(proxies))
	for _, p := range proxies {
		p.GenerateHash()
		key := string(p.Hash)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			unique = append(unique, p)
		}
	}
	return unique
}

func calculateBatchSize(proxyCount int) int {
	if proxyCount <= batchThreshold {
		return proxyCount
	}

	numFields, err := getNumDatabaseFields(models.Proxy{}, DB)
	if err != nil || numFields == 0 {
		return minBatchSize // Fallback to safe minimum
	}

	batchSize := maxParamsPerBatch / numFields
	return clamp(batchSize, minBatchSize, proxyCount)
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func insertProxies(tx *gorm.DB, proxies []models.Proxy, batchSize int) error {
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		DoNothing: true,
	}).CreateInBatches(proxies, batchSize).Error
}

func fetchExistingProxies(tx *gorm.DB, proxies []models.Proxy, batchSize int) ([]models.Proxy, error) {
	hashes := make([][]byte, len(proxies))
	for i, p := range proxies {
		hashes[i] = p.Hash
	}

	var results []models.Proxy
	for i := 0; i < len(hashes); i += batchSize {
		end := i + batchSize
		if end > len(hashes) {
			end = len(hashes)
		}

		var batch []models.Proxy
		err := tx.Preload("Users").
			Where("hash IN ?", hashes[i:end]).
			Find(&batch).Error
		if err != nil {
			return nil, err
		}
		results = append(results, batch...)
	}
	return results, nil
}

func createUserAssociations(tx *gorm.DB, proxies []models.Proxy, userID uint, batchSize int) error {
	associations := make([]models.UserProxy, len(proxies))
	for i, p := range proxies {
		associations[i] = models.UserProxy{
			UserID:  userID,
			ProxyID: p.ID,
		}
	}

	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "proxy_id"}},
		DoNothing: true,
	}).CreateInBatches(associations, batchSize).Error
}

func fetchProxiesWithUsers(tx *gorm.DB, proxies []models.Proxy) ([]models.Proxy, error) {
	ids := make([]uint64, len(proxies))
	for i, p := range proxies {
		ids[i] = p.ID
	}

	var results []models.Proxy
	for i := 0; i < len(ids); i += maxParamsPerBatch {
		end := i + maxParamsPerBatch
		if end > len(ids) {
			end = len(ids)
		}

		var batch []models.Proxy
		err := tx.Preload("Users").
			Where("id IN ?", ids[i:end]).
			Find(&batch).Error
		if err != nil {
			return nil, err
		}
		results = append(results, batch...)
	}
	return results, nil
}

func transactionRollbackHandler(tx *gorm.DB) {
	if r := recover(); r != nil {
		tx.Rollback()
		log.Errorf("Transaction rolled back due to panic: %v", r)
	}
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

func GetAllProxies() ([]models.Proxy, error) {
	var allProxies []models.Proxy
	const batchSize = maxParamsPerBatch

	collectedProxies := make([]models.Proxy, 0)

	err := DB.Preload("Users").Order("id").FindInBatches(&allProxies, batchSize, func(tx *gorm.DB, batch int) error {
		collectedProxies = append(collectedProxies, allProxies...)
		return nil
	})

	if err.Error != nil {
		return nil, err.Error
	}

	return collectedProxies, nil
}

func GetProxyPage(userId uint, page int) []routeModels.ProxyInfo {
	offset := (page - 1) * proxiesPerPage

	subQuery := DB.Model(&models.ProxyStatistic{}).
		Select("DISTINCT ON (proxy_id) *").
		Order("proxy_id, created_at DESC")

	var results []routeModels.ProxyInfo

	DB.Model(&models.Proxy{}).
		Select(
			"CONCAT(proxies.ip1, '.', proxies.ip2, '.', proxies.ip3, '.', proxies.ip4, ':', proxies.port) AS ip, "+
				"COALESCE(ps.estimated_type, 'N/A') AS estimated_type, "+
				"COALESCE(ps.response_time, 0) AS response_time, "+
				"COALESCE(ps.country, 'N/A') AS country, "+
				"COALESCE(al.name, 'N/A') AS anonymity_level, "+
				"COALESCE(pr.name, 'N/A') AS protocol, "+
				"COALESCE(ps.alive, false) AS alive, "+
				"COALESCE(ps.created_at, '0001-01-01 00:00:00'::timestamp) AS latest_check",
		).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.user_id = ?", userId).
		Joins("LEFT JOIN (?) AS ps ON ps.proxy_id = proxies.id", subQuery).
		Joins("LEFT JOIN anonymity_levels al ON al.id = ps.level_id").
		Joins("LEFT JOIN protocols pr ON pr.id = ps.protocol_id").
		Order("alive DESC, latest_check DESC").
		Offset(offset).
		Limit(proxiesPerPage).
		Scan(&results)

	return results
}
