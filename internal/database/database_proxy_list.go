package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

const proxiesPerPage = 40

const maxProxiesPerPage = 100

type ProxyPageQueryOptions struct {
	IncludeHealth     bool
	IncludeReputation bool
	SortField         string
	SortOrder         string
}

func GetWorkspaceProxyCount(workspaceID uint) int64 {
	var count int64
	DB.Model(&domain.ManagedProxy{}).
		Where("workspace_id = ?", workspaceID).
		Count(&count)
	return count
}

func GetProxyInfoPage(workspaceID uint, page int) []dto.ProxyInfo {
	proxies, _ := GetProxyInfoPageWithCount(workspaceID, page)
	return proxies
}

func GetProxyInfoPageWithCount(workspaceID uint, page int) ([]dto.ProxyInfo, int64) {
	return GetProxyInfoPageWithFilters(workspaceID, page, proxiesPerPage, "", dto.ProxyListFilters{})
}

func GetProxyInfoPageWithFilters(workspaceID uint, page int, pageSize int, search string, filters dto.ProxyListFilters) ([]dto.ProxyInfo, int64) {
	return GetProxyInfoPageWithFiltersAndOptions(workspaceID, page, pageSize, search, filters, ProxyPageQueryOptions{
		IncludeHealth:     true,
		IncludeReputation: true,
	})
}

func GetProxyInfoPageWithFiltersAndOptions(
	workspaceID uint,
	page int,
	pageSize int,
	search string,
	filters dto.ProxyListFilters,
	options ProxyPageQueryOptions,
) ([]dto.ProxyInfo, int64) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 || pageSize > maxProxiesPerPage {
		pageSize = proxiesPerPage
	}

	options = normalizeProxyPageQueryOptions(options)

	query := DB.Table("user_proxy_filter_indexes ufi").
		Select(
			"ufi.proxy_id AS id, "+
				"ufi.state AS state, "+
				"ufi.pause_reason AS pause_reason, "+
				"ufi.host AS ip_address, "+
				"ufi.port AS port, "+
				"ufi.estimated_type AS estimated_type, "+
				"CASE WHEN cw.checker_dirty THEN 0 ELSE ufi.response_time END AS response_time, "+
				"ufi.country AS country, "+
				"CASE WHEN cw.checker_dirty THEN 'N/A' ELSE ufi.anonymity_level END AS anonymity_level, "+
				"CASE WHEN cw.checker_dirty THEN FALSE ELSE ufi.alive END AS alive, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_overall END AS health_overall, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_http END AS health_http, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_https END AS health_https, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_socks4 END AS health_socks4, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_socks5 END AS health_socks5, "+
				"ufi.latest_check AS latest_check, cw.checker_dirty AS checker_pending",
		).
		Joins("JOIN workspaces cw ON cw.id = ufi.workspace_id").
		Where("ufi.workspace_id = ?", workspaceID)

	query = applyProxyPageSort(query, options)

	filterQuery := buildProxyListFilterQuery(workspaceID, filters)
	if filterQuery != nil {
		query = query.Where("ufi.proxy_id IN (?)", filterQuery)
	}

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
		if options.IncludeReputation {
			attachReputationsToProxyInfos(proxies)
		}
		attachProxyTagsForResponse(workspaceID, proxies)
		if filterQuery == nil {
			total := GetWorkspaceProxyCount(workspaceID)
			return proxies, total
		}

		var total int64
		if err := DB.Table("(?) AS filtered", filterQuery.Select("ufi.proxy_id").Group("ufi.proxy_id")).Count(&total).Error; err != nil {
			return proxies, 0
		}
		return proxies, total
	}

	if !isLikelyProxyIPSearch(lowerSearch) {
		matchedProxyIDs := buildProxySearchIDQuery(workspaceID, filterQuery, lowerSearch)

		var total int64
		if err := DB.Table("(?) AS matched", matchedProxyIDs).Count(&total).Error; err != nil {
			return []dto.ProxyInfo{}, 0
		}

		if total == 0 {
			return []dto.ProxyInfo{}, 0
		}

		offset := (page - 1) * pageSize
		if err := query.
			Joins("JOIN (?) AS matched ON matched.id = ufi.proxy_id", matchedProxyIDs).
			Offset(offset).
			Limit(pageSize).
			Scan(&rows).Error; err != nil {
			return []dto.ProxyInfo{}, 0
		}

		proxies := proxyInfoRowsToDTO(rows)
		if options.IncludeReputation {
			attachReputationsToProxyInfos(proxies)
		}
		attachProxyTagsForResponse(workspaceID, proxies)
		return proxies, total
	}

	network, fallbackPrefix, exact, ok := buildIPSearchNetwork(lowerSearch)
	if !ok {
		return []dto.ProxyInfo{}, 0
	}

	matchedProxyIDs := buildProxyIPSearchIDQuery(workspaceID, filterQuery, network, fallbackPrefix, lowerSearch, exact)

	var total int64
	if err := DB.Table("(?) AS matched", matchedProxyIDs).Count(&total).Error; err != nil {
		return []dto.ProxyInfo{}, 0
	}
	if total == 0 {
		return []dto.ProxyInfo{}, 0
	}

	offset := (page - 1) * pageSize
	if err := query.
		Joins("JOIN (?) AS matched ON matched.id = ufi.proxy_id", matchedProxyIDs).
		Offset(offset).
		Limit(pageSize).
		Scan(&rows).Error; err != nil {
		return []dto.ProxyInfo{}, 0
	}

	proxies := proxyInfoRowsToDTO(rows)
	if options.IncludeReputation {
		attachReputationsToProxyInfos(proxies)
	}
	attachProxyTagsForResponse(workspaceID, proxies)
	return proxies, total
}

func attachProxyTagsForResponse(workspaceID uint, proxies []dto.ProxyInfo) {
	if err := AttachProxyTagsToInfos(workspaceID, proxies); err != nil {
		log.Error("failed to attach proxy tags", "error", err, "workspace_id", workspaceID)
	}
}

func normalizeProxyPageQueryOptions(options ProxyPageQueryOptions) ProxyPageQueryOptions {
	sortField := normalizeProxyPageSortField(options.SortField)
	sortOrder := normalizeProxyPageSortOrder(options.SortOrder)
	if sortField == "" || sortOrder == "" {
		sortField = ""
		sortOrder = ""
	}

	return ProxyPageQueryOptions{
		IncludeHealth:     options.IncludeHealth || proxyPageSortNeedsHealth(sortField),
		IncludeReputation: options.IncludeReputation || sortField == "reputation",
		SortField:         sortField,
		SortOrder:         sortOrder,
	}
}

func normalizeProxyPageSortField(field string) string {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "alive":
		return "alive"
	case "health_overall", "alive_ratio_overall":
		return "health_overall"
	case "health_http", "alive_ratio_http":
		return "health_http"
	case "health_https", "alive_ratio_https":
		return "health_https"
	case "health_socks4", "alive_ratio_socks4":
		return "health_socks4"
	case "health_socks5", "alive_ratio_socks5":
		return "health_socks5"
	case "ip":
		return "ip"
	case "ip_port":
		return "ip_port"
	case "port":
		return "port"
	case "response_time":
		return "response_time"
	case "estimated_type":
		return "estimated_type"
	case "country":
		return "country"
	case "reputation":
		return "reputation"
	case "latest_check":
		return "latest_check"
	default:
		return ""
	}
}

func normalizeProxyPageSortOrder(order string) string {
	switch strings.ToLower(strings.TrimSpace(order)) {
	case "asc", "1":
		return "asc"
	case "desc", "-1":
		return "desc"
	default:
		return ""
	}
}

func proxyPageSortNeedsHealth(field string) bool {
	return field == "health_overall" ||
		field == "health_http" ||
		field == "health_https" ||
		field == "health_socks4" ||
		field == "health_socks5"
}

func applyProxyPageSort(query *gorm.DB, options ProxyPageQueryOptions) *gorm.DB {
	if options.SortField == "" || options.SortOrder == "" {
		return query.Order("ufi.alive DESC, ufi.latest_check DESC, ufi.proxy_id ASC")
	}

	direction := "ASC"
	if options.SortOrder == "desc" {
		direction = "DESC"
	}

	expressions := proxyPageSortExpressions(options.SortField)
	if len(expressions) == 0 {
		return query.Order("ufi.alive DESC, ufi.latest_check DESC, ufi.proxy_id ASC")
	}

	orderParts := make([]string, 0, len(expressions)+1)
	for _, expression := range expressions {
		orderParts = append(orderParts, fmt.Sprintf("%s %s NULLS LAST", expression, direction))
	}
	orderParts = append(orderParts, "ufi.proxy_id ASC")

	return query.Order(strings.Join(orderParts, ", "))
}

func proxyPageSortExpressions(field string) []string {
	switch field {
	case "alive":
		return []string{"ufi.alive"}
	case "health_overall":
		return []string{"ufi.health_overall"}
	case "health_http":
		return []string{"ufi.health_http"}
	case "health_https":
		return []string{"ufi.health_https"}
	case "health_socks4":
		return []string{"ufi.health_socks4"}
	case "health_socks5":
		return []string{"ufi.health_socks5"}
	case "ip":
		return []string{"ufi.host"}
	case "ip_port":
		return []string{"ufi.host", "ufi.port"}
	case "port":
		return []string{"ufi.port"}
	case "response_time":
		return []string{"ufi.response_time"}
	case "estimated_type":
		return []string{"ufi.estimated_type"}
	case "country":
		return []string{"ufi.country"}
	case "reputation":
		return []string{"ufi.reputation_score"}
	case "latest_check":
		return []string{"ufi.latest_check"}
	default:
		return nil
	}
}

func proxyInfoRowsToDTO(rows []dto.ProxyInfoRow) []dto.ProxyInfo {
	results := make([]dto.ProxyInfo, 0, len(rows))
	for _, row := range rows {
		if row.CheckerPending {
			row.LatestCheck = time.Time{}
		}

		results = append(results, dto.ProxyInfo{
			Id:             row.Id,
			State:          row.State,
			PauseReason:    row.PauseReason,
			IP:             row.IPAddress,
			Port:           row.Port,
			EstimatedType:  row.EstimatedType,
			ResponseTime:   row.ResponseTime,
			Country:        row.Country,
			AnonymityLevel: row.AnonymityLevel,
			HealthKnown:    !row.LatestCheck.IsZero(),
			Alive:          row.Alive,
			Health:         buildHealthSummary(row),
			LatestCheck:    row.LatestCheck,
		})
	}

	return results
}

func nullFloat32Pointer(value sql.NullFloat64) *float32 {
	if !value.Valid {
		return nil
	}

	return new(float32(value.Float64))
}

func buildHealthSummary(row dto.ProxyInfoRow) *dto.ProxyHealthSummary {
	summary := &dto.ProxyHealthSummary{
		Overall: nullFloat32Pointer(row.HealthOverall),
		HTTP:    nullFloat32Pointer(row.HealthHTTP),
		HTTPS:   nullFloat32Pointer(row.HealthHTTPS),
		SOCKS4:  nullFloat32Pointer(row.HealthSOCKS4),
		SOCKS5:  nullFloat32Pointer(row.HealthSOCKS5),
	}

	if summary.Overall == nil && summary.HTTP == nil && summary.HTTPS == nil && summary.SOCKS4 == nil && summary.SOCKS5 == nil {
		return nil
	}

	return summary
}
