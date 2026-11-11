package evidence

import (
	"fmt"
	"gorm.io/gorm"
	"math"
	"strings"

	"magpie/internal/domain"
)

var (
	reputationLabelOrder = []string{"good", "neutral", "poor"}
	reputationLabelSet   = map[string]struct{}{
		"good":    {},
		"neutral": {},
		"poor":    {},
	}
)

const (
	UptimeFilterMin = "min"
	UptimeFilterMax = "max"
)

func CountAliveProxiesForProtocol(tx *gorm.DB, workspaceID uint, protocolID int, labels []string, uptimeFilterType string, uptimePercentage *float64) (int, error) {
	var count int64
	err := BuildAliveProxyQuery(tx, workspaceID, protocolID, labels, uptimeFilterType, uptimePercentage).
		Select("COUNT(DISTINCT proxies.id)").Scan(&count).Error
	return int(count), err
}

func BuildAliveProxyQuery(tx *gorm.DB, workspaceID uint, protocolID int, labels []string, uptimeFilterType string, uptimePercentage *float64) *gorm.DB {
	filterLabels := SanitizeRotatorReputationLabels(labels)

	// Scope latest evidence to this workspace before joining shared ownership.
	// Joining the global current-evidence view expands every route's other owners.
	query := tx.
		Model(&domain.Proxy{}).
		Select("proxies.*").
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.workspace_id = ? AND up.state = ?", workspaceID, domain.ManagedProxyStateActive).
		Joins("JOIN workspaces checker_workspace ON checker_workspace.id = up.workspace_id AND NOT checker_workspace.checker_dirty").
		Joins("JOIN proxy_latest_statistics pls ON pls.proxy_id = proxies.id AND pls.workspace_id = CASE WHEN checker_workspace.checker_generation = 0 THEN 0 ELSE checker_workspace.id END AND pls.protocol_id = ? AND pls.alive = ? AND (pls.transport_protocol = 'tcp' OR pls.workspace_id = 0)", protocolID, true).
		Joins("LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = up.workspace_id AND cp.proxy_id = proxies.id AND cp.protocol_id = pls.protocol_id").
		Where(`(checker_workspace.checker_generation = 0 OR
			(checker_workspace.checker_generation > 0 AND pls.config_key <> '' AND
			pls.config_key = COALESCE(cp.config_key,
				CASE pls.protocol_id WHEN 1 THEN checker_workspace.checker_http_key WHEN 2 THEN checker_workspace.checker_https_key WHEN 3 THEN checker_workspace.checker_socks4_key WHEN 4 THEN checker_workspace.checker_socks5_key ELSE '' END)))`)

	query = applyReputationFilter(query, filterLabels)
	query = applyUptimeFilter(query, tx, protocolID, uptimeFilterType, uptimePercentage)
	return query
}

func applyReputationFilter(query *gorm.DB, labels []string) *gorm.DB {
	if !shouldApplyReputationFilter(labels) {
		return query
	}

	return query.
		Joins("JOIN proxy_reputations pr ON pr.proxy_id = proxies.id AND pr.kind = ?", domain.ProxyReputationKindOverall).
		Where("pr.label IN ?", labels)
}

func applyUptimeFilter(query *gorm.DB, tx *gorm.DB, protocolID int, uptimeFilterType string, uptimePercentage *float64) *gorm.DB {
	normalizedType, normalizedPercentage := NormalizeRotatorUptimeFilter(uptimeFilterType, uptimePercentage)
	if normalizedType == "" || normalizedPercentage == nil {
		return query
	}

	// History records one physical request. Expand its workspace evidence so a
	// shared request can have different validation outcomes in each workspace.
	evidenceTable := "jsonb_to_recordset(COALESCE(psr.check_evidence, '[]'::jsonb)) AS e(workspace_id bigint, config_key text, alive boolean)"
	workspaceExpr, keyExpr, aliveExpr := "e.workspace_id", "e.config_key", "e.alive"
	if tx.Dialector.Name() == "sqlite" {
		evidenceTable = "json_each(COALESCE(psr.check_evidence, '[]')) e"
		workspaceExpr = "json_extract(e.value, '$.workspace_id')"
		keyExpr = "json_extract(e.value, '$.config_key')"
		aliveExpr = "json_extract(e.value, '$.alive')"
	}
	// Correlate history before expanding evidence. A candidate cannot scan
	// retained history belonging to unrelated routes or workspaces.
	evidenceUptime := "SELECT ROUND(100.0 * SUM(CASE WHEN " + aliveExpr + " THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) " +
		"FROM proxy_statistics psr CROSS JOIN " + evidenceTable +
		" WHERE psr.proxy_id = proxies.id AND psr.protocol_id = ? AND psr.transport_protocol = 'tcp' " +
		"AND " + workspaceExpr + " = pls.workspace_id AND " + keyExpr + " = pls.config_key"
	legacyUptime := "SELECT ROUND(100.0 * SUM(CASE WHEN psr.alive THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) " +
		"FROM proxy_statistics psr WHERE psr.proxy_id = proxies.id AND psr.protocol_id = ?"
	comparison := ">="
	if normalizedType == UptimeFilterMax {
		comparison = "<="
	}
	return query.Where("(CASE WHEN pls.workspace_id = 0 THEN ("+legacyUptime+") ELSE ("+evidenceUptime+") END) "+comparison+" ?",
		protocolID, protocolID, *normalizedPercentage)
}

func shouldApplyReputationFilter(labels []string) bool {
	return len(labels) > 0 && len(labels) < len(reputationLabelOrder)
}

func SanitizeRotatorReputationLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(labels))
	for _, raw := range labels {
		label := strings.ToLower(strings.TrimSpace(raw))
		if _, ok := reputationLabelSet[label]; ok {
			seen[label] = struct{}{}
		}
	}

	if len(seen) == 0 {
		return nil
	}

	result := make([]string, 0, len(seen))
	for _, label := range reputationLabelOrder {
		if _, ok := seen[label]; ok {
			result = append(result, label)
		}
	}
	return result
}

func NormalizeRotatorUptimeFilter(rawType string, rawPercentage *float64) (string, *float64) {
	filterType := strings.ToLower(strings.TrimSpace(rawType))
	if rawPercentage == nil {
		return "", nil
	}
	if filterType != UptimeFilterMin && filterType != UptimeFilterMax {
		return "", nil
	}

	value := *rawPercentage
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
		return "", nil
	}

	return filterType, new(math.Round(value*10) / 10)
}

func BuildAliveProxyCacheKey(protocolID int, labels []string, uptimeFilterType string, uptimePercentage *float64) string {
	labelKey := "*"
	if len(labels) > 0 {
		labelKey = strings.Join(labels, ",")
	}

	uptimeType, uptimeValue := NormalizeRotatorUptimeFilter(uptimeFilterType, uptimePercentage)
	uptimeKey := "*"
	if uptimeType != "" && uptimeValue != nil {
		uptimeKey = fmt.Sprintf("%s:%0.1f", uptimeType, *uptimeValue)
	}

	return fmt.Sprintf("%d:%s:%s", protocolID, labelKey, uptimeKey)
}
