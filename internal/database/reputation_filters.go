package database

import (
	"strings"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

// applyReputationFilters narrows proxy queries to entries whose overall reputation labels match any of the provided labels.
// Accepts "good", "neutral", "poor", and "unknown" (case-insensitive). "unknown" also covers proxies without reputation rows.
func applyReputationFilters(query *gorm.DB, labels []string) *gorm.DB {
	if len(labels) == 0 {
		return query
	}

	normalizedSet := make(map[string]struct{})
	normalized := make([]string, 0, len(labels))
	includeUnknown := false

	for _, label := range labels {
		trimmed := strings.ToLower(strings.TrimSpace(label))
		if trimmed == "" {
			continue
		}
		if trimmed == "unknown" {
			includeUnknown = true
			continue
		}
		if _, exists := normalizedSet[trimmed]; exists {
			continue
		}
		normalizedSet[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	finalLabels := make([]string, 0, len(normalized)+1)
	finalLabels = append(finalLabels, normalized...)
	if includeUnknown {
		finalLabels = append(finalLabels, "unknown")
	}

	if len(finalLabels) == 0 {
		return query
	}

	expr := "LOWER(COALESCE(NULLIF(proxy_reputations.label, ''), 'unknown'))"

	return query.
		Joins("LEFT JOIN proxy_reputations ON proxy_reputations.proxy_id = proxies.id AND proxy_reputations.kind = ?", domain.ProxyReputationKindOverall).
		Where(expr+" IN ?", finalLabels)
}
