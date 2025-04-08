package database

import (
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/models"
	"magpie/models/routeModels"
	"strings"
)

const (
	batchThreshold    = 8191  // Use batches when exceeding this number of records
	maxParamsPerBatch = 65534 // Conservative default (PostgreSQL's limit) - 1
	minBatchSize      = 100   // Minimum batch size to maintain efficiency

	proxiesPerPage = 40
)

func InsertAndGetProxies(proxies []models.Proxy, userID uint) ([]models.Proxy, error) {
	uniqueProxies := deduplicateProxies(proxies)
	if len(uniqueProxies) == 0 {
		return nil, nil
	}

	batchSize := calculateBatchSize(len(uniqueProxies))

	tx := DB.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer transactionRollbackHandler(tx)

	// Insert proxies and populate their IDs (including existing ones)
	if err := insertProxies(tx, uniqueProxies, batchSize); err != nil {
		return nil, err
	}

	// Create associations using the now-populated IDs
	if err := createUserAssociations(tx, uniqueProxies, userID, batchSize); err != nil {
		return nil, err
	}

	proxiesWithUsers, err := fetchProxiesWithUsers(tx, uniqueProxies)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit().Error; err != nil {
		return nil, err
	}

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
		DoUpdates: clause.AssignmentColumns([]string{"hash"}), // To get the ids from duplicates
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
			"proxies.id AS id, "+
				"CONCAT(proxies.ip1, '.', proxies.ip2, '.', proxies.ip3, '.', proxies.ip4) AS ip, "+
				"proxies.port AS port, "+
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

func DeleteProxyRelation(userId uint, proxies []int) {
	DB.Where("proxy_id IN (?)", proxies).Where("user_id = (?)", userId).Delete(&models.UserProxy{})
}

// GetProxiesForExport retrieves proxies from the database based on export settings
func GetProxiesForExport(userID uint, settings routeModels.ExportSettings) ([]models.Proxy, error) {
	var proxies []models.Proxy

	// Create a base query that applies proxy status regardless of filter setting
	baseQuery := DB.Preload("Statistics", func(db *gorm.DB) *gorm.DB {
		return db.Order("created_at DESC").Limit(1)
	}).Preload("Statistics.Protocol").
		Joins("JOIN user_proxies ON user_proxies.proxy_id = proxies.id").
		Where("user_proxies.user_id = ?", userID)

	// Apply proxy status filter regardless of the main filter flag
	if settings.ProxyStatus == "alive" || settings.ProxyStatus == "dead" {
		isAlive := settings.ProxyStatus == "alive"
		baseQuery = baseQuery.Joins("JOIN proxy_statistics ON proxies.id = proxy_statistics.proxy_id").
			Where("proxy_statistics.alive = ?", isAlive).
			Group("proxies.id, proxy_statistics.id")

		// For dead/alive status, make sure we're getting the latest statistic that matches the status
		baseQuery = baseQuery.Where("proxy_statistics.created_at = (SELECT MAX(ps.created_at) FROM proxy_statistics ps WHERE ps.proxy_id = proxies.id)")
	}

	// Apply specific proxy IDs if provided
	if len(settings.Proxies) > 0 {
		baseQuery = baseQuery.Where("proxies.id IN ?", settings.Proxies)
	}

	// If filter is enabled, apply additional filters
	if settings.Filter {
		return applyAdditionalFilters(baseQuery, settings)
	} else {
		// Just use the base query with status filter
		err := baseQuery.Find(&proxies).Error
		return proxies, err
	}
}

// applyAdditionalFilters applies additional filters based on settings
func applyAdditionalFilters(query *gorm.DB, settings routeModels.ExportSettings) ([]models.Proxy, error) {
	var proxies []models.Proxy

	// Apply protocol filters
	if settings.Http || settings.Https || settings.Socks4 || settings.Socks5 {
		var protocols []string
		if settings.Http {
			protocols = append(protocols, "http")
		}
		if settings.Https {
			protocols = append(protocols, "https")
		}
		if settings.Socks4 {
			protocols = append(protocols, "socks4")
		}
		if settings.Socks5 {
			protocols = append(protocols, "socks5")
		}

		// Make sure we have the proxy_statistics join
		if !strings.Contains(query.Statement.SQL.String(), "JOIN proxy_statistics") {
			query = query.Joins("JOIN proxy_statistics ON proxies.id = proxy_statistics.proxy_id")
		}

		query = query.Joins("JOIN protocols ON proxy_statistics.protocol_id = protocols.id").
			Where("protocols.name IN ?", protocols)
	}

	// Apply response time filter
	if settings.MaxTimeout > 0 {
		// Make sure we have the proxy_statistics join
		if !strings.Contains(query.Statement.SQL.String(), "JOIN proxy_statistics") {
			query = query.Joins("JOIN proxy_statistics ON proxies.id = proxy_statistics.proxy_id")
		}
		query = query.Where("proxy_statistics.response_time <= ?", settings.MaxTimeout)
	}

	// Apply retry count filter
	if settings.MaxRetries > 0 {
		// Make sure we have the proxy_statistics join
		if !strings.Contains(query.Statement.SQL.String(), "JOIN proxy_statistics") {
			query = query.Joins("JOIN proxy_statistics ON proxies.id = proxy_statistics.proxy_id")
		}
		query = query.Where("proxy_statistics.attempt <= ?", settings.MaxRetries)
	}

	// Ensure proper grouping for the query
	if strings.Contains(query.Statement.SQL.String(), "JOIN proxy_statistics") {
		query = query.Group("proxies.id, proxy_statistics.id")

		// Add protocols to grouping if they're joined
		if strings.Contains(query.Statement.SQL.String(), "JOIN protocols") {
			query = query.Group("proxies.id, proxy_statistics.id, protocols.id")
		}
	}

	err := query.Find(&proxies).Error
	return proxies, err
}
