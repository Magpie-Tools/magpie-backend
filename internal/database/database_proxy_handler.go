package database

import (
	"time"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/security"
)

const (
	batchThreshold    = 8191  // Use batches when exceeding this number of records
	maxParamsPerBatch = 65534 // Conservative default (PostgreSQL's limit) - 1
	minBatchSize      = 100   // Minimum batch size to maintain efficiency

	proxiesPerPage = 40
)

func InsertAndGetProxies(proxies []domain.Proxy, userIDs ...uint) ([]domain.Proxy, error) {
	if len(proxies) == 0 || len(userIDs) == 0 {
		return nil, nil
	}

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

	// Create associations for each user
	for _, userID := range userIDs {
		if err := createUserAssociations(tx, uniqueProxies, userID, batchSize); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	return uniqueProxies, nil
}

// InsertAndGetProxyIDs behaves like InsertAndGetProxies, but it returns the
// primary-key of every (new *and* existing) proxy instead of the full structs.
func InsertAndGetProxyIDs(proxies []domain.Proxy, userIDs ...uint) ([]uint64, error) {
	inserted, err := InsertAndGetProxies(proxies, userIDs...)
	if err != nil || len(inserted) == 0 {
		return nil, err
	}

	var missingHashes [][]byte
	for _, p := range inserted {
		if p.ID == 0 {
			missingHashes = append(missingHashes, p.Hash)
		}
	}

	if len(missingHashes) > 0 {
		var existing []struct {
			ID   uint64
			Hash []byte
		}
		if err := DB.
			Select("id, hash").
			Where("hash IN ?", missingHashes).
			Find(&existing).Error; err != nil {
			return nil, err
		}

		hashToID := make(map[string]uint64, len(existing))
		for _, e := range existing {
			hashToID[string(e.Hash)] = e.ID
		}

		for i, p := range inserted {
			if p.ID == 0 {
				if id, ok := hashToID[string(p.Hash)]; ok {
					inserted[i].ID = id
				}
			}
		}
	}

	out := make([]uint64, len(inserted))
	for i, p := range inserted {
		out[i] = p.ID
	}
	return out, nil
}

func InsertAndGetProxiesWithUser(proxies []domain.Proxy, userIDs ...uint) ([]domain.Proxy, error) {
	if len(proxies) == 0 || len(userIDs) == 0 {
		return nil, nil
	}

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

	// Create associations for each user
	for _, userID := range userIDs {
		if err := createUserAssociations(tx, uniqueProxies, userID, batchSize); err != nil {
			return nil, err
		}
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
func deduplicateProxies(proxies []domain.Proxy) []domain.Proxy {
	seen := make(map[string]struct{}, len(proxies))
	unique := make([]domain.Proxy, 0, len(proxies))
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

	numFields, err := getNumDatabaseFields(domain.Proxy{}, DB)
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

func insertProxies(tx *gorm.DB, proxies []domain.Proxy, batchSize int) error {
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		DoUpdates: clause.AssignmentColumns([]string{"hash"}), // To get the ids from duplicates
	}).CreateInBatches(proxies, batchSize).Error
}

func fetchExistingProxies(tx *gorm.DB, proxies []domain.Proxy, batchSize int) ([]domain.Proxy, error) {
	hashes := make([][]byte, len(proxies))
	for i, p := range proxies {
		hashes[i] = p.Hash
	}

	var results []domain.Proxy
	for i := 0; i < len(hashes); i += batchSize {
		end := i + batchSize
		if end > len(hashes) {
			end = len(hashes)
		}

		var batch []domain.Proxy
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

func createUserAssociations(tx *gorm.DB, proxies []domain.Proxy, userID uint, batchSize int) error {
	associations := make([]domain.UserProxy, len(proxies))
	for i, p := range proxies {
		associations[i] = domain.UserProxy{
			UserID:  userID,
			ProxyID: p.ID,
		}
	}

	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "proxy_id"}},
		DoNothing: true,
	}).CreateInBatches(associations, batchSize).Error
}

func fetchProxiesWithUsers(tx *gorm.DB, proxies []domain.Proxy) ([]domain.Proxy, error) {
	ids := make([]uint64, len(proxies))
	for i, p := range proxies {
		ids[i] = p.ID
	}

	var results []domain.Proxy
	for i := 0; i < len(ids); i += maxParamsPerBatch {
		end := i + maxParamsPerBatch
		if end > len(ids) {
			end = len(ids)
		}

		var batch []domain.Proxy
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
	DB.Model(&domain.Proxy{}).Count(&count)
	return count
}

func GetAllProxyCountOfUser(userId uint) int64 {
	var count int64
	DB.Model(&domain.Proxy{}).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.user_id = ?", userId).
		Count(&count)
	return count
}

func GetAllProxies() ([]domain.Proxy, error) {
	var allProxies []domain.Proxy
	const batchSize = maxParamsPerBatch

	collectedProxies := make([]domain.Proxy, 0)

	err := DB.Preload("Users").Order("id").FindInBatches(&allProxies, batchSize, func(tx *gorm.DB, batch int) error {
		collectedProxies = append(collectedProxies, allProxies...)
		return nil
	})

	if err.Error != nil {
		return nil, err.Error
	}

	return collectedProxies, nil
}

func GetProxyInfoPage(userId uint, page int) []dto.ProxyInfo {
	offset := (page - 1) * proxiesPerPage

	subQuery := DB.Model(&domain.ProxyStatistic{}).
		Select("DISTINCT ON (proxy_id) *").
		Order("proxy_id, created_at DESC")

	type proxyInfoRow struct {
		Id             int       `gorm:"column:id"`
		IPEncrypted    string    `gorm:"column:ip_encrypted"`
		Port           uint16    `gorm:"column:port"`
		EstimatedType  string    `gorm:"column:estimated_type"`
		ResponseTime   int16     `gorm:"column:response_time"`
		Country        string    `gorm:"column:country"`
		AnonymityLevel string    `gorm:"column:anonymity_level"`
		Protocol       string    `gorm:"column:protocol"`
		Alive          bool      `gorm:"column:alive"`
		LatestCheck    time.Time `gorm:"column:latest_check"`
	}

	rows := make([]proxyInfoRow, 0, proxiesPerPage)

	DB.Model(&domain.Proxy{}).
		Select(
			"proxies.id AS id, "+
				"proxies.ip AS ip_encrypted, "+
				"proxies.port AS port, "+
				"COALESCE(NULLIF(proxies.estimated_type, ''), 'N/A') AS estimated_type, "+
				"COALESCE(ps.response_time, 0) AS response_time, "+
				"COALESCE(NULLIF(proxies.country, ''), 'N/A') AS country, "+
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
		Scan(&rows)

	results := make([]dto.ProxyInfo, 0, len(rows))
	for _, row := range rows {
		ip, _, err := security.DecryptProxySecret(row.IPEncrypted)
		if err != nil {
			log.Errorf("decrypt proxy ip: %v", err)
			ip = ""
		}

		results = append(results, dto.ProxyInfo{
			Id:             row.Id,
			IP:             ip,
			Port:           row.Port,
			EstimatedType:  row.EstimatedType,
			ResponseTime:   row.ResponseTime,
			Country:        row.Country,
			AnonymityLevel: row.AnonymityLevel,
			Protocol:       row.Protocol,
			Alive:          row.Alive,
			LatestCheck:    row.LatestCheck,
		})
	}

	return results
}

func DeleteProxyRelation(userId uint, proxies []int) {
	DB.Where("proxy_id IN (?)", proxies).Where("user_id = (?)", userId).Delete(&domain.UserProxy{})
}

func GetProxiesForExport(userID uint, settings dto.ExportSettings) ([]domain.Proxy, error) {
	var proxies []domain.Proxy

	baseQuery := DB.Preload("Statistics", func(db *gorm.DB) *gorm.DB {
		return db.Order("created_at DESC")
	}).Preload("Statistics.Protocol").
		Joins("JOIN user_proxies ON user_proxies.proxy_id = proxies.id").
		Where("user_proxies.user_id = ?", userID)

	if settings.ProxyStatus == "alive" || settings.ProxyStatus == "dead" {
		isAlive := settings.ProxyStatus == "alive"
		// Use subquery to check latest proxy_statistics.alive status
		baseQuery = baseQuery.Where(
			"(SELECT ps.alive FROM proxy_statistics ps WHERE ps.proxy_id = proxies.id ORDER BY ps.created_at DESC LIMIT 1) = ?",
			isAlive,
		)
	}

	if len(settings.Proxies) > 0 {
		baseQuery = baseQuery.Where("proxies.id IN ?", settings.Proxies)
	}

	if settings.Filter {
		return applyAdditionalFilters(baseQuery, settings)
	} else {
		err := baseQuery.Find(&proxies).Error
		return proxies, err
	}
}

// applyAdditionalFilters applies additional filters based on settings
func applyAdditionalFilters(query *gorm.DB, settings dto.ExportSettings) ([]domain.Proxy, error) {
	var proxies []domain.Proxy

	// If any of the filters require proxy_statistics, join it once.
	needsProxyStatistics := settings.Http || settings.Https || settings.Socks4 || settings.Socks5 || settings.MaxTimeout > 0 || settings.MaxRetries > 0
	if needsProxyStatistics {
		query = query.Joins("JOIN proxy_statistics ON proxies.id = proxy_statistics.proxy_id")
	}

	// Apply protocol filters using the protocols join if any protocols are selected.
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
		// Add the join for protocols once.
		query = query.Joins("JOIN protocols ON proxy_statistics.protocol_id = protocols.id").
			Where("protocols.name IN ?", protocols)
	}

	// Apply response time filter.
	if settings.MaxTimeout > 0 {
		query = query.Where("proxy_statistics.response_time <= ?", settings.MaxTimeout)
	}

	// Apply retry count filter.
	if settings.MaxRetries > 0 {
		query = query.Where("proxy_statistics.attempt <= ?", settings.MaxRetries)
	}

	// Group the results to avoid duplicates.
	// In many cases, grouping by the primary key (proxies.id) is sufficient.
	// If you joined protocols and proxy_statistics then you may need to group by those IDs as well.
	groupBy := "proxies.id"
	if settings.Http || settings.Https || settings.Socks4 || settings.Socks5 {
		groupBy += ", protocols.id"
	}
	// Optionally include the proxy_statistics.id if needed to ensure uniqueness.
	groupBy += ", proxy_statistics.id"
	query = query.Group(groupBy)

	err := query.Find(&proxies).Error
	return proxies, err
}
