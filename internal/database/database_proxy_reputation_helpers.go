package database

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

func attachReputationsToProxyInfos(proxies []dto.ProxyInfo) {
	if len(proxies) == 0 {
		return
	}

	proxyIDs := make([]uint64, 0, len(proxies))
	for _, proxy := range proxies {
		if proxy.Id <= 0 {
			continue
		}
		proxyIDs = append(proxyIDs, uint64(proxy.Id))
	}

	if len(proxyIDs) == 0 {
		return
	}

	repMap, err := GetProxyReputations(context.Background(), proxyIDs)
	if err != nil {
		log.Error("failed to load proxy reputations", "error", err)
		return
	}

	missing := make([]uint64, 0)
	dedupMissing := make(map[uint64]struct{}, len(proxies))

	for index := range proxies {
		id := uint64(proxies[index].Id)
		if rows, ok := repMap[id]; ok && len(rows) > 0 {
			proxies[index].Reputation = mapReputationsToSummary(rows)
			continue
		}

		if id == 0 {
			continue
		}

		if _, seen := dedupMissing[id]; !seen {
			dedupMissing[id] = struct{}{}
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 {
		scheduleReputationRecalculation(missing)
	}
}

func mapReputationsToSummary(rows []domain.ProxyReputation) *dto.ProxyReputationSummary {
	if len(rows) == 0 {
		return nil
	}

	summary := &dto.ProxyReputationSummary{
		Protocols: make(map[string]dto.ProxyReputation),
	}

	for _, row := range rows {
		rep := dto.ProxyReputation{
			Kind:  row.Kind,
			Score: row.Score,
			Label: row.Label,
		}

		if row.Kind == domain.ProxyReputationKindOverall {
			summary.Overall = new(rep)
		} else {
			summary.Protocols[row.Kind] = rep
		}
	}

	if len(summary.Protocols) == 0 {
		summary.Protocols = nil
	}

	return summary
}

func mapReputationsToBreakdown(rows []domain.ProxyReputation) *dto.ProxyReputationBreakdown {
	if len(rows) == 0 {
		return nil
	}

	breakdown := &dto.ProxyReputationBreakdown{
		Protocols: make(map[string]dto.ProxyReputationDetail),
	}

	for _, row := range rows {
		signals := decodeReputationSignals(row.Signals)
		rep := dto.ProxyReputationDetail{
			Kind:    row.Kind,
			Score:   row.Score,
			Label:   row.Label,
			Signals: signals,
		}

		if row.Kind == domain.ProxyReputationKindOverall {
			breakdown.Overall = new(rep)
		} else {
			breakdown.Protocols[row.Kind] = rep
		}
	}

	if len(breakdown.Protocols) == 0 {
		breakdown.Protocols = nil
	}

	return breakdown
}

func decodeReputationSignals(payload []byte) map[string]any {
	if len(payload) == 0 {
		return nil
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		log.Error("failed to decode reputation signals", "error", err)
		return nil
	}

	return decoded
}

func scheduleReputationRecalculation(proxyIDs []uint64) {
	if len(proxyIDs) == 0 {
		return
	}

	ids := append([]uint64(nil), proxyIDs...)

	go func(values []uint64) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := RecalculateProxyReputations(ctx, values); err != nil {
			log.Error("failed to backfill proxy reputations", "error", err, "proxy_ids", values)
		}
	}(ids)
}

func normalizeReputationLabels(labels []string) (map[string]struct{}, bool) {
	allowed := make(map[string]struct{}, len(labels))
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
		allowed[trimmed] = struct{}{}
	}

	return allowed, includeUnknown
}

func protocolsForExport(settings dto.ExportSettings) []string {
	if !settings.Filter {
		return nil
	}

	protocols := make([]string, 0, 4)
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

	return protocols
}

func applyListReputationFilters(query *gorm.DB, labels []string, protocols []string) *gorm.DB {
	allowedSet, includeUnknown := normalizeReputationLabels(labels)
	if len(allowedSet) == 0 && !includeUnknown {
		return query
	}

	targetKinds := targetReputationKindsForProtocols(protocols)
	if len(targetKinds) == 0 {
		return query
	}

	keys := setToSlice(allowedSet)
	labelExpr := "LOWER(COALESCE(NULLIF(pr.label, ''), 'unknown'))"

	if includeUnknown {
		query = query.Joins("LEFT JOIN proxy_reputations pr ON pr.proxy_id = ufi.proxy_id AND LOWER(pr.kind) IN ?", targetKinds)
		if len(keys) > 0 {
			query = query.Where(labelExpr+" IN ? OR pr.id IS NULL OR "+labelExpr+" = 'unknown'", keys)
		} else {
			query = query.Where("pr.id IS NULL OR " + labelExpr + " = 'unknown'")
		}
	} else {
		query = query.Joins("JOIN proxy_reputations pr ON pr.proxy_id = ufi.proxy_id AND LOWER(pr.kind) IN ?", targetKinds)
		if len(keys) > 0 {
			query = query.Where(labelExpr+" IN ?", keys)
		}
	}

	return query
}

func targetReputationKindsForProtocols(protocols []string) []string {
	if len(protocols) == 0 {
		return []string{domain.ProxyReputationKindOverall}
	}

	out := make([]string, 0, len(protocols))
	for _, proto := range protocols {
		if trimmed := strings.ToLower(strings.TrimSpace(proto)); trimmed != "" {
			out = append(out, trimmed)
		}
	}

	if len(out) == 0 {
		return []string{domain.ProxyReputationKindOverall}
	}

	return out
}

func setToSlice(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	return out
}

func proxyMatchesReputationFilters(proxy domain.Proxy, allowed map[string]struct{}, includeUnknown bool, selectedProtocols []string) bool {
	if len(allowed) == 0 && !includeUnknown {
		return true
	}

	reputations := make(map[string]domain.ProxyReputation, len(proxy.Reputations))
	for _, rep := range proxy.Reputations {
		reputations[strings.ToLower(rep.Kind)] = rep
	}

	targetProtocols := make([]string, 0, len(selectedProtocols))
	for _, proto := range selectedProtocols {
		if trimmed := strings.ToLower(strings.TrimSpace(proto)); trimmed != "" {
			targetProtocols = append(targetProtocols, trimmed)
		}
	}

	if len(targetProtocols) == 0 {
		if len(proxy.Statistics) > 0 {
			proto := strings.ToLower(strings.TrimSpace(proxy.Statistics[0].Protocol.Name))
			if proto != "" {
				targetProtocols = append(targetProtocols, proto)
			}
		}
		if len(targetProtocols) == 0 {
			targetProtocols = append(targetProtocols, domain.ProxyReputationKindOverall)
		}
	}

	for _, proto := range targetProtocols {
		rep, ok := reputations[proto]
		if ok {
			label := strings.ToLower(strings.TrimSpace(rep.Label))
			if label == "" {
				label = "unknown"
			}
			if _, match := allowed[label]; match {
				return true
			}
			if label == "unknown" && includeUnknown {
				return true
			}
			continue
		}

		if includeUnknown {
			return true
		}
	}

	return false
}
