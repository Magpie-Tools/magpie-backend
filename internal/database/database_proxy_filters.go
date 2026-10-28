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

	query, args := proxyFilterOptionValuesQuery(DB, workspaceID)
	var rows []proxyFilterValueRow
	err := DB.Raw(query, args...).Scan(&rows).Error
	if err != nil {
		return dto.ProxyFilterOptions{}, err
	}
	countries, types, anonymityLevels := make([]string, 0), make([]string, 0), make([]string, 0)
	for _, row := range rows {
		if strings.TrimSpace(row.Value) == "" {
			continue
		}
		switch row.Kind {
		case "country":
			countries = append(countries, row.Value)
		case "type":
			types = append(types, row.Value)
		case "anonymity":
			anonymityLevels = append(anonymityLevels, row.Value)
		}
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
	Kind  string `gorm:"column:kind"`
	Value string `gorm:"column:value"`
}

func proxyFilterOptionValuesQuery(db *gorm.DB, workspaceID uint) (string, []any) {
	sources := []struct{ kind, column string }{
		{"country", "country"}, {"type", "estimated_type"}, {"anonymity", "anonymity_level"},
	}
	branches := make([]string, 0, len(sources))
	ctes := make([]string, 0, len(sources))
	args := make([]any, 0, len(sources)*2)
	for _, source := range sources {
		value := "COALESCE(NULLIF(ufi." + source.column + ", ''), 'N/A')"
		if !isPostgresDialect(db) {
			branches = append(branches, fmt.Sprintf("SELECT DISTINCT '%s' AS kind, %s AS value FROM user_proxy_filter_indexes ufi WHERE ufi.workspace_id = ?", source.kind, value))
			args = append(args, workspaceID)
			continue
		}
		// Each successor is one index seek past the previous distinct value.
		// Reading all matching index entries still scales with millions of routes.
		name := source.column + "_values"
		ctes = append(ctes, fmt.Sprintf(`%s(value) AS (
			(SELECT %s FROM user_proxy_filter_indexes ufi WHERE ufi.workspace_id = ? ORDER BY %s LIMIT 1)
			UNION ALL
			SELECT (SELECT %s FROM user_proxy_filter_indexes ufi
				WHERE ufi.workspace_id = ? AND %s > previous.value ORDER BY %s LIMIT 1)
			FROM %s previous WHERE previous.value IS NOT NULL
		)`, name, value, value, value, value, value, name))
		branches = append(branches, fmt.Sprintf("SELECT '%s' AS kind, value FROM %s WHERE value IS NOT NULL", source.kind, name))
		args = append(args, workspaceID, workspaceID)
	}
	query := strings.Join(branches, " UNION ALL ") + " ORDER BY kind, value"
	if len(ctes) > 0 {
		query = "WITH RECURSIVE " + strings.Join(ctes, ", ") + " " + query
	}
	return query, args
}
