package database

import (
	"fmt"
	"strings"

	"magpie/internal/api/dto"

	"gorm.io/gorm"
)

func buildProxyListFilterQuery(workspaceID uint, filters dto.ProxyListFilters) *gorm.DB {
	if !hasProxyListFilters(filters) {
		return nil
	}

	selectedProtocols := normalizeProtocolFilters(filters.Protocols)

	query := DB.Table("user_proxy_filter_indexes ufi").
		Select("ufi.proxy_id").
		Where("ufi.workspace_id = ?", workspaceID)

	if filters.Status == "alive" || filters.Status == "dead" || hasProxyHealthFilters(filters) || len(selectedProtocols) > 0 || len(filters.AnonymityLevels) > 0 || filters.MaxTimeout > 0 || filters.MaxRetries > 0 {
		query = query.Where("EXISTS (SELECT 1 FROM workspaces w WHERE w.id = ufi.workspace_id AND NOT w.checker_dirty)")
	}

	if states := filters.LifecycleStates(); len(states) > 0 {
		query = query.Where("ufi.state IN ?", states)
	}

	if filters.Status == "alive" || filters.Status == "dead" {
		if filters.Status == "alive" {
			query = query.Where("ufi.alive = ?", true)
		} else {
			query = query.Where("ufi.alive = ? AND ufi.health_overall IS NOT NULL", false)
		}
	}

	if len(filters.Countries) > 0 {
		query = query.Where("ufi.country_key IN ?", filters.Countries)
	}

	if len(filters.Types) > 0 {
		query = query.Where("ufi.type_key IN ?", filters.Types)
	}

	if hasProxyHealthFilters(filters) {
		if filters.MinHealthOverall > 0 {
			query = query.Where("COALESCE(ufi.health_overall, -1) >= ?", filters.MinHealthOverall)
		}

		if filters.MinHealthHTTP > 0 {
			query = query.Where("COALESCE(ufi.health_http, -1) >= ?", filters.MinHealthHTTP)
		}

		if filters.MinHealthHTTPS > 0 {
			query = query.Where("COALESCE(ufi.health_https, -1) >= ?", filters.MinHealthHTTPS)
		}

		if filters.MinHealthSOCKS4 > 0 {
			query = query.Where("COALESCE(ufi.health_socks4, -1) >= ?", filters.MinHealthSOCKS4)
		}

		if filters.MinHealthSOCKS5 > 0 {
			query = query.Where("COALESCE(ufi.health_socks5, -1) >= ?", filters.MinHealthSOCKS5)
		}
	}

	if len(filters.AnonymityLevels) > 0 {
		query = query.Where("ufi.anonymity_key IN ?", filters.AnonymityLevels)
	}

	if filters.MaxTimeout > 0 {
		query = query.Where("ufi.response_time <= ?", filters.MaxTimeout)
	}

	if filters.MaxRetries > 0 {
		query = query.Where("ufi.attempt <= ?", filters.MaxRetries)
	}

	if len(selectedProtocols) > 0 {
		for _, protocol := range selectedProtocols {
			switch protocol {
			case "http":
				query = query.Where("ufi.alive_http = ?", true)
			case "https":
				query = query.Where("ufi.alive_https = ?", true)
			case "socks4":
				query = query.Where("ufi.alive_socks4 = ?", true)
			case "socks5":
				query = query.Where("ufi.alive_socks5 = ?", true)
			}
		}
	}

	if len(filters.ReputationLabels) > 0 {
		query = applyListReputationFilters(query, filters.ReputationLabels, selectedProtocols)
	}

	if len(filters.TagIDs) > 0 {
		query = query.Where(
			"EXISTS (SELECT 1 FROM proxy_tag_assignments pta JOIN proxy_tags pt ON pt.id = pta.proxy_tag_id AND pt.workspace_id = pta.workspace_id WHERE pta.workspace_id = ufi.workspace_id AND pta.proxy_id = ufi.proxy_id AND pta.proxy_tag_id IN ?)",
			filters.TagIDs,
		)
	}

	return query.Group("ufi.proxy_id")
}

func normalizeProtocolFilters(protocols []string) []string {
	if len(protocols) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(protocols))
	out := make([]string, 0, len(protocols))
	for _, protocol := range protocols {
		normalized := strings.ToLower(strings.TrimSpace(protocol))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	return out
}

func hasProxyListFilters(filters dto.ProxyListFilters) bool {
	if len(filters.LifecycleStates()) > 0 {
		return true
	}
	if filters.Status == "alive" || filters.Status == "dead" {
		return true
	}
	if hasProxyHealthFilters(filters) {
		return true
	}
	if filters.MaxTimeout > 0 || filters.MaxRetries > 0 {
		return true
	}
	if len(filters.Protocols) > 0 || len(filters.Countries) > 0 || len(filters.Types) > 0 || len(filters.AnonymityLevels) > 0 || len(filters.ReputationLabels) > 0 || len(filters.TagIDs) > 0 {
		return true
	}
	return false
}

func hasProxyHealthFilters(filters dto.ProxyListFilters) bool {
	return filters.MinHealthOverall > 0 ||
		filters.MinHealthHTTP > 0 ||
		filters.MinHealthHTTPS > 0 ||
		filters.MinHealthSOCKS4 > 0 ||
		filters.MinHealthSOCKS5 > 0
}

func GetProxyFilterOptions(workspaceID uint) (dto.ProxyFilterOptions, error) {
	if DB == nil {
		return dto.ProxyFilterOptions{}, fmt.Errorf("database connection was not initialised")
	}

	countries, err := loadDistinctProxyInfoValue(workspaceID, "ufi.country")
	if err != nil {
		return dto.ProxyFilterOptions{}, err
	}

	types, err := loadDistinctProxyInfoValue(workspaceID, "ufi.estimated_type")
	if err != nil {
		return dto.ProxyFilterOptions{}, err
	}

	anonymityLevels, err := loadDistinctAnonymityLevels(workspaceID)
	if err != nil {
		return dto.ProxyFilterOptions{}, err
	}

	tags, err := GetProxyTags(workspaceID)
	if err != nil {
		return dto.ProxyFilterOptions{}, err
	}

	return dto.ProxyFilterOptions{
		Countries:       countries,
		Types:           types,
		AnonymityLevels: anonymityLevels,
		Tags:            tags,
	}, nil
}

type proxyFilterValueRow struct {
	Value string `gorm:"column:value"`
}

func loadDistinctProxyInfoValue(workspaceID uint, column string) ([]string, error) {
	var rows []proxyFilterValueRow
	if err := DB.Table("user_proxy_filter_indexes ufi").
		Select("DISTINCT COALESCE(NULLIF("+column+", ''), 'N/A') AS value").
		Where("ufi.workspace_id = ?", workspaceID).
		Order("value").
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	return extractProxyFilterValues(rows), nil
}

func loadDistinctAnonymityLevels(workspaceID uint) ([]string, error) {
	var rows []proxyFilterValueRow
	if err := DB.Table("user_proxy_filter_indexes ufi").
		Select("DISTINCT COALESCE(NULLIF(ufi.anonymity_level, ''), 'N/A') AS value").
		Where("ufi.workspace_id = ?", workspaceID).
		Order("value").
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	return extractProxyFilterValues(rows), nil
}

func extractProxyFilterValues(rows []proxyFilterValueRow) []string {
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.Value) == "" {
			continue
		}
		values = append(values, row.Value)
	}
	return values
}
