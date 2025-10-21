package database

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/security"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	batchThreshold    = 8191  // Use batches when exceeding this number of records
	maxParamsPerBatch = 65534 // Conservative default (PostgreSQL's limit) - 1
	minBatchSize      = 100   // Minimum batch size to maintain efficiency
	deleteChunkSize   = 5000  // Keep large deletes under Postgres parameter limits

	proxiesPerPage    = 40
	maxProxiesPerPage = 100
)

var ErrNoProxiesSelected = errors.New("no proxies selected for deletion")

func InsertAndGetProxies(proxies []domain.Proxy, userIDs ...uint) ([]domain.Proxy, error) {
	return insertAndAssociateProxies(proxies, userIDs)
}

func InsertAndGetProxiesWithUser(proxies []domain.Proxy, userIDs ...uint) ([]domain.Proxy, error) {
	inserted, err := insertAndAssociateProxies(proxies, userIDs)
	if err != nil || len(inserted) == 0 {
		return inserted, err
	}

	proxiesWithUsers, err := fetchProxiesWithUsers(DB, inserted)
	if err != nil {
		return nil, err
	}

	return proxiesWithUsers, nil
}

func insertAndAssociateProxies(proxies []domain.Proxy, userIDs []uint) ([]domain.Proxy, error) {
	if len(proxies) == 0 || len(userIDs) == 0 {
		return nil, nil
	}

	uniqueProxies := deduplicateProxies(proxies)
	if len(uniqueProxies) == 0 {
		return nil, nil
	}

	batchSize := calculateBatchSize(len(uniqueProxies))
	limitCfg := config.GetConfig().ProxyLimits

	tx := DB.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer transactionRollbackHandler(tx)

	perUserHashes := make(map[uint][]string, len(userIDs))
	allowedHashes := make(map[string]struct{})

	for _, userID := range userIDs {
		hashes, err := filterHashesForUser(tx, uniqueProxies, userID, batchSize, limitCfg)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		if len(hashes) == 0 {
			continue
		}
		perUserHashes[userID] = hashes
		for _, hash := range hashes {
			allowedHashes[hash] = struct{}{}
		}
	}

	if len(allowedHashes) == 0 {
		tx.Rollback()
		return nil, nil
	}

	uniqueProxies = filterProxiesByHash(uniqueProxies, allowedHashes)

	if err := insertProxies(tx, uniqueProxies, batchSize); err != nil {
		tx.Rollback()
		return nil, err
	}

	if err := ensureProxyIDs(tx, uniqueProxies); err != nil {
		tx.Rollback()
		return nil, err
	}

	hashToID := make(map[string]uint64, len(uniqueProxies))
	for i := range uniqueProxies {
		hashToID[string(uniqueProxies[i].Hash)] = uniqueProxies[i].ID
	}

	for _, userID := range userIDs {
		hashes := perUserHashes[userID]
		if len(hashes) == 0 {
			continue
		}

		proxyIDs := make([]uint64, 0, len(hashes))
		for _, hash := range hashes {
			if id, ok := hashToID[hash]; ok {
				proxyIDs = append(proxyIDs, id)
			}
		}
		if len(proxyIDs) == 0 {
			continue
		}

		if err := createUserAssociations(tx, proxyIDs, userID, batchSize); err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	return uniqueProxies, nil
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

func filterHashesForUser(tx *gorm.DB, proxies []domain.Proxy, userID uint, chunkSize int, limitCfg config.ProxyLimitConfig) ([]string, error) {
	if len(proxies) == 0 {
		return nil, nil
	}

	if !limitCfg.Enabled {
		return collectHashes(proxies), nil
	}

	if limitCfg.ExcludeAdmins {
		var role string
		if err := tx.Model(&domain.User{}).
			Select("role").
			Where("id = ?", userID).
			Scan(&role).Error; err != nil {
			return nil, err
		}
		if role == "admin" {
			return collectHashes(proxies), nil
		}
	}

	existingSet, err := getExistingHashesForUser(tx, userID, proxies, chunkSize)
	if err != nil {
		return nil, err
	}

	var currentCount int64
	if err := tx.Table("user_proxies").
		Where("user_id = ?", userID).
		Count(&currentCount).Error; err != nil {
		return nil, err
	}

	available := int64(limitCfg.MaxPerUser) - currentCount
	if available < 0 {
		available = 0
	}

	allowed := make([]string, 0, len(proxies))
	for _, proxy := range proxies {
		key := string(proxy.Hash)
		if _, ok := existingSet[key]; ok {
			allowed = append(allowed, key)
			continue
		}
		if available == 0 {
			continue
		}
		allowed = append(allowed, key)
		available--
	}

	return allowed, nil
}

func collectHashes(proxies []domain.Proxy) []string {
	if len(proxies) == 0 {
		return nil
	}

	hashes := make([]string, len(proxies))
	for i, proxy := range proxies {
		hashes[i] = string(proxy.Hash)
	}
	return hashes
}

func getExistingHashesForUser(tx *gorm.DB, userID uint, proxies []domain.Proxy, chunkSize int) (map[string]struct{}, error) {
	existing := make(map[string]struct{}, len(proxies))
	if len(proxies) == 0 {
		return existing, nil
	}

	if chunkSize <= 0 || chunkSize > maxParamsPerBatch {
		chunkSize = maxParamsPerBatch
		if len(proxies) < chunkSize {
			chunkSize = len(proxies)
		}
		if chunkSize == 0 {
			chunkSize = minBatchSize
		}
	}

	hashes := make([][]byte, len(proxies))
	for i, proxy := range proxies {
		hashes[i] = proxy.Hash
	}

	for i := 0; i < len(hashes); i += chunkSize {
		end := i + chunkSize
		if end > len(hashes) {
			end = len(hashes)
		}

		var rows [][]byte
		err := tx.Table("user_proxies up").
			Joins("JOIN proxies p ON up.proxy_id = p.id").
			Where("up.user_id = ? AND p.hash IN ?", userID, hashes[i:end]).
			Pluck("p.hash", &rows).Error
		if err != nil {
			return nil, err
		}

		for _, hash := range rows {
			existing[string(hash)] = struct{}{}
		}
	}

	return existing, nil
}

func filterProxiesByHash(proxies []domain.Proxy, allowed map[string]struct{}) []domain.Proxy {
	if len(allowed) == 0 {
		return nil
	}

	filtered := proxies[:0]
	for _, proxy := range proxies {
		if _, ok := allowed[string(proxy.Hash)]; ok {
			filtered = append(filtered, proxy)
		}
	}
	return filtered
}

func ensureProxyIDs(tx *gorm.DB, proxies []domain.Proxy) error {
	var missing [][]byte
	for _, proxy := range proxies {
		if proxy.ID == 0 {
			missing = append(missing, proxy.Hash)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	var results []struct {
		ID   uint64
		Hash []byte
	}
	if err := tx.Model(&domain.Proxy{}).
		Select("id, hash").
		Where("hash IN ?", missing).
		Find(&results).Error; err != nil {
		return err
	}

	lookup := make(map[string]uint64, len(results))
	for _, r := range results {
		lookup[string(r.Hash)] = r.ID
	}

	for i, proxy := range proxies {
		if proxy.ID != 0 {
			continue
		}
		if id, ok := lookup[string(proxy.Hash)]; ok {
			proxies[i].ID = id
		}
	}

	return nil
}

func createUserAssociations(tx *gorm.DB, proxyIDs []uint64, userID uint, batchSize int) error {
	if len(proxyIDs) == 0 {
		return nil
	}

	associations := make([]domain.UserProxy, len(proxyIDs))
	for i, id := range proxyIDs {
		associations[i] = domain.UserProxy{
			UserID:  userID,
			ProxyID: id,
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
	proxies, _ := GetProxyInfoPageWithFilters(userId, page, proxiesPerPage, "")
	return proxies
}

func GetProxyInfoPageWithFilters(userId uint, page int, pageSize int, search string) ([]dto.ProxyInfo, int64) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 || pageSize > maxProxiesPerPage {
		pageSize = proxiesPerPage
	}

	subQuery := DB.Model(&domain.ProxyStatistic{}).
		Select("DISTINCT ON (proxy_id) *").
		Order("proxy_id, created_at DESC")

	query := DB.Model(&domain.Proxy{}).
		Select(
			"proxies.id AS id, "+
				"proxies.ip AS ip_encrypted, "+
				"proxies.port AS port, "+
				"COALESCE(NULLIF(proxies.estimated_type, ''), 'N/A') AS estimated_type, "+
				"COALESCE(ps.response_time, 0) AS response_time, "+
				"COALESCE(NULLIF(proxies.country, ''), 'N/A') AS country, "+
				"COALESCE(al.name, 'N/A') AS anonymity_level, "+
				"COALESCE(ps.alive, false) AS alive, "+
				"COALESCE(ps.created_at, '0001-01-01 00:00:00'::timestamp) AS latest_check",
		).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.user_id = ?", userId).
		Joins("LEFT JOIN (?) AS ps ON ps.proxy_id = proxies.id", subQuery).
		Joins("LEFT JOIN anonymity_levels al ON al.id = ps.level_id").
		Order("alive DESC, latest_check DESC")

	rows := make([]dto.ProxyInfoRow, 0)
	normalizedSearch := strings.TrimSpace(search)
	lowerSearch := strings.ToLower(normalizedSearch)

	if normalizedSearch == "" {
		offset := (page - 1) * pageSize
		query = query.Offset(offset).Limit(pageSize)
		if err := query.Scan(&rows).Error; err != nil {
			return []dto.ProxyInfo{}, 0
		}

		proxies := proxyInfoRowsToDTO(rows)
		total := GetAllProxyCountOfUser(userId)
		return proxies, total
	}

	// Proxy IPs are stored encrypted, so the search needs to run after decrypting
	// the values that came back from the database. We therefore filter in-memory
	// once the full result set for the user has been loaded.
	if err := query.Scan(&rows).Error; err != nil {
		return []dto.ProxyInfo{}, 0
	}

	proxies := proxyInfoRowsToDTO(rows)
	filtered := filterProxiesBySearch(proxies, lowerSearch)
	total := int64(len(filtered))
	start := (page - 1) * pageSize
	if start >= len(filtered) {
		return []dto.ProxyInfo{}, total
	}

	end := start + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}

	return filtered[start:end], total
}

func proxyInfoRowsToDTO(rows []dto.ProxyInfoRow) []dto.ProxyInfo {
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
			Alive:          row.Alive,
			LatestCheck:    row.LatestCheck,
		})
	}

	return results
}

func filterProxiesBySearch(proxies []dto.ProxyInfo, search string) []dto.ProxyInfo {
	if search == "" {
		return proxies
	}

	filtered := make([]dto.ProxyInfo, 0, len(proxies))
	for _, proxy := range proxies {
		if proxyMatchesSearch(proxy, search) {
			filtered = append(filtered, proxy)
		}
	}

	return filtered
}

func proxyMatchesSearch(proxy dto.ProxyInfo, search string) bool {
	if strings.Contains(strings.ToLower(proxy.IP), search) {
		return true
	}

	if strings.Contains(strconv.FormatUint(uint64(proxy.Port), 10), search) {
		return true
	}

	if strings.Contains(strings.ToLower(proxy.EstimatedType), search) {
		return true
	}

	if strings.Contains(strings.ToLower(proxy.Country), search) {
		return true
	}

	if strings.Contains(strings.ToLower(proxy.AnonymityLevel), search) {
		return true
	}

	if strings.Contains(strconv.Itoa(int(proxy.ResponseTime)), search) {
		return true
	}

	aliveLabel := "dead"
	if proxy.Alive {
		aliveLabel = "alive"
	}
	if strings.Contains(aliveLabel, search) {
		return true
	}

	if !proxy.LatestCheck.IsZero() {
		if strings.Contains(strings.ToLower(proxy.LatestCheck.Format(time.RFC3339)), search) {
			return true
		}
	}

	return false
}

func GetProxyDetail(userId uint, proxyId uint64) (*dto.ProxyDetail, error) {
	if proxyId == 0 {
		return nil, nil
	}

	var proxy domain.Proxy
	err := DB.
		Preload("Statistics", func(db *gorm.DB) *gorm.DB {
			return db.
				Order("created_at DESC").
				Limit(1).
				Preload("Protocol").
				Preload("Level").
				Preload("Judge")
		}).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id").
		Where("up.user_id = ? AND proxies.id = ?", userId, proxyId).
		First(&proxy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var latestStat *dto.ProxyStatistic
	var latestCheck *time.Time
	if len(proxy.Statistics) > 0 {
		mapped := mapProxyStatistic(&proxy.Statistics[0])
		latestStat = &mapped
		latestCheck = &proxy.Statistics[0].CreatedAt
	}

	detail := &dto.ProxyDetail{
		Id:              int(proxy.ID),
		IP:              proxy.GetIp(),
		Port:            proxy.Port,
		Username:        proxy.Username,
		Password:        proxy.Password,
		HasAuth:         proxy.HasAuth(),
		EstimatedType:   normaliseDisplayValue(proxy.EstimatedType, "N/A"),
		Country:         normaliseDisplayValue(proxy.Country, "Unknown"),
		CreatedAt:       proxy.CreatedAt,
		LatestCheck:     latestCheck,
		LatestStatistic: latestStat,
	}

	return detail, nil
}

func GetProxyStatistics(userId uint, proxyId uint64, limit int) ([]dto.ProxyStatistic, error) {
	if proxyId == 0 {
		return []dto.ProxyStatistic{}, nil
	}

	if limit <= 0 || limit > 500 {
		limit = 500
	}

	query := DB.Model(&domain.ProxyStatistic{}).
		Preload("Protocol").
		Preload("Level").
		Preload("Judge").
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Where("proxy_statistics.proxy_id = ? AND up.user_id = ?", proxyId, userId).
		Order("proxy_statistics.created_at DESC").
		Limit(limit)

	rows := make([]domain.ProxyStatistic, 0, limit)
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}

	stats := make([]dto.ProxyStatistic, len(rows))
	for index := range rows {
		stats[index] = mapProxyStatistic(&rows[index])
	}

	return stats, nil
}

type proxyStatisticBodyRow struct {
	ResponseBody string
	Regex        sql.NullString
}

func GetProxyStatisticResponseBody(userId uint, proxyId uint64, statisticId uint64) (dto.ProxyStatisticDetail, error) {
	if proxyId == 0 || statisticId == 0 {
		return dto.ProxyStatisticDetail{}, gorm.ErrRecordNotFound
	}

	var row proxyStatisticBodyRow
	err := DB.Table("proxy_statistics").
		Select("proxy_statistics.response_body", "user_judges.regex").
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Joins("LEFT JOIN user_judges ON user_judges.judge_id = proxy_statistics.judge_id AND user_judges.user_id = up.user_id").
		Where("proxy_statistics.id = ? AND proxy_statistics.proxy_id = ? AND up.user_id = ?", statisticId, proxyId, userId).
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dto.ProxyStatisticDetail{}, gorm.ErrRecordNotFound
		}
		return dto.ProxyStatisticDetail{}, err
	}

	regex := ""
	if row.Regex.Valid {
		regex = strings.TrimSpace(row.Regex.String)
	}

	return dto.ProxyStatisticDetail{
		ResponseBody: row.ResponseBody,
		Regex:        regex,
	}, nil
}

func mapProxyStatistic(stat *domain.ProxyStatistic) dto.ProxyStatistic {
	if stat == nil {
		return dto.ProxyStatistic{}
	}

	protocol := normaliseDisplayValue(stat.Protocol.Name, "Unknown")
	anonymity := normaliseDisplayValue(stat.Level.Name, "Unknown")
	judge := normaliseDisplayValue(stat.Judge.FullString, "Unknown")

	return dto.ProxyStatistic{
		Id:             stat.ID,
		Alive:          stat.Alive,
		Attempt:        stat.Attempt,
		ResponseTime:   stat.ResponseTime,
		ResponseBody:   stat.ResponseBody,
		Protocol:       protocol,
		AnonymityLevel: anonymity,
		Judge:          judge,
		CreatedAt:      stat.CreatedAt,
	}
}

func normaliseDisplayValue(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func DeleteProxyRelation(userId uint, proxies []int) (int64, error) {
	if len(proxies) == 0 {
		return 0, nil
	}

	var totalDeleted int64
	chunkSize := deleteChunkSize
	if chunkSize > len(proxies) {
		chunkSize = len(proxies)
	}
	if chunkSize <= 0 {
		chunkSize = len(proxies)
	}

	for start := 0; start < len(proxies); start += chunkSize {
		end := start + chunkSize
		if end > len(proxies) {
			end = len(proxies)
		}

		chunk := proxies[start:end]
		result := DB.
			Where("user_id = ?", userId).
			Where("proxy_id IN ?", chunk).
			Delete(&domain.UserProxy{})

		if result.Error != nil {
			return totalDeleted, result.Error
		}

		totalDeleted += result.RowsAffected
	}

	return totalDeleted, nil
}

func DeleteProxiesWithSettings(userID uint, settings dto.DeleteSettings) (int64, error) {
	if settings.Scope == "selected" && len(settings.Proxies) == 0 {
		return 0, ErrNoProxiesSelected
	}

	proxyIDs, err := collectProxyIDsForDeletion(userID, settings)
	if err != nil {
		return 0, err
	}

	if len(proxyIDs) == 0 {
		return 0, nil
	}

	intIDs := make([]int, 0, len(proxyIDs))
	for _, id := range proxyIDs {
		intIDs = append(intIDs, int(id))
	}

	return DeleteProxyRelation(userID, intIDs)
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
