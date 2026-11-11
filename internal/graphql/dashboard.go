package graphql

import (
	"context"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/database"
)

const (
	defaultViewerProxyHistoryLimit = 168
	maxViewerProxyHistoryLimit     = 720
	maxViewerProxySnapshotLimit    = 720
	defaultRecentProxyChecksLimit  = 8
	maxRecentProxyChecksLimit      = 50
	defaultFastestAliveProxyLimit  = 100
	maxFastestAliveProxyLimit      = 100
)

func buildProxyHistory(workspaceID uint, limit int) []map[string]interface{} {
	entries := database.GetProxyHistoryEntries(workspaceID, limit)
	result := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		result = append(result, map[string]interface{}{
			"count":      entry.Count,
			"recordedAt": entry.RecordedAt,
		})
	}
	return result
}

func buildRecentProxyChecks(workspaceID uint, limit int) []map[string]interface{} {
	value, _ := buildRecentProxyChecksContext(context.Background(), workspaceID, limit)
	return value
}

func buildRecentProxyChecksContext(ctx context.Context, workspaceID uint, limit int) ([]map[string]interface{}, error) {
	entries, err := database.GetRecentProxyChecksContext(ctx, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		var latestCheck interface{}
		if !entry.LatestCheck.IsZero() {
			latestCheck = entry.LatestCheck
		}
		result = append(result, map[string]interface{}{
			"id":           int(entry.ID),
			"ip":           entry.IP,
			"port":         int(entry.Port),
			"responseTime": int(entry.ResponseTime),
			"alive":        entry.Alive,
			"latestCheck":  latestCheck,
		})
	}
	return result, nil
}

func buildFastestAliveProxies(workspaceID uint, limit int) []map[string]interface{} {
	value, _ := buildFastestAliveProxiesContext(context.Background(), workspaceID, limit)
	return value
}

func buildFastestAliveProxiesContext(ctx context.Context, workspaceID uint, limit int) ([]map[string]interface{}, error) {
	entries, err := database.GetFastestAliveProxiesContext(ctx, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		var latestCheck interface{}
		if !entry.LatestCheck.IsZero() {
			latestCheck = entry.LatestCheck
		}
		result = append(result, map[string]interface{}{
			"id":              int(entry.ID),
			"ip":              entry.IP,
			"port":            int(entry.Port),
			"responseTime":    int(entry.ResponseTime),
			"country":         entry.Country,
			"reputationLabel": entry.ReputationLabel,
			"reputationScore": float64(entry.ReputationScore),
			"latestCheck":     latestCheck,
		})
	}
	return result, nil
}

func buildProxySnapshots(entries []dto.ProxySnapshotEntry) []map[string]interface{} {
	if len(entries) == 0 {
		return []map[string]interface{}{}
	}

	result := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		result = append(result, map[string]interface{}{
			"count":      entry.Count,
			"recordedAt": entry.RecordedAt,
		})
	}
	return result
}

func ensureLatestAliveSnapshot(entries []dto.ProxySnapshotEntry, currentCount int64) []dto.ProxySnapshotEntry {
	now := time.Now()
	if len(entries) == 0 {
		return append(entries, dto.ProxySnapshotEntry{
			Count:      currentCount,
			RecordedAt: now,
		})
	}

	lastIndex := len(entries) - 1
	if entries[lastIndex].Count == currentCount {
		entries[lastIndex].RecordedAt = now
		return entries
	}

	return append(entries, dto.ProxySnapshotEntry{
		Count:      currentCount,
		RecordedAt: now,
	})
}

func clampPositiveLimit(limit, max int) int {
	if limit <= 0 {
		return 0
	}
	if limit > max {
		return max
	}
	return limit
}

func buildDashboard(info dto.DashboardInfo) map[string]interface{} {
	countries := make([]map[string]interface{}, 0, len(info.CountryBreakdown))
	for _, entry := range info.CountryBreakdown {
		countries = append(countries, map[string]interface{}{
			"country": entry.Country,
			"count":   int(entry.Count),
		})
	}

	reputation := map[string]interface{}{
		"good":    int(info.ReputationBreakdown.Good),
		"neutral": int(info.ReputationBreakdown.Neutral),
		"poor":    int(info.ReputationBreakdown.Poor),
		"unknown": int(info.ReputationBreakdown.Unknown),
	}

	entries := make([]map[string]interface{}, 0, len(info.JudgeValidProxies))
	for _, entry := range info.JudgeValidProxies {
		entries = append(entries, map[string]interface{}{
			"judgeUrl":           entry.JudgeUrl,
			"eliteProxies":       int(entry.EliteProxies),
			"anonymousProxies":   int(entry.AnonymousProxies),
			"transparentProxies": int(entry.TransparentProxies),
		})
	}

	result := map[string]interface{}{
		"totalChecks":         int(info.TotalChecks),
		"totalScraped":        int(info.TotalScraped),
		"totalChecksWeek":     int(info.TotalChecksWeek),
		"totalScrapedWeek":    int(info.TotalScrapedWeek),
		"reputationBreakdown": reputation,
		"countryBreakdown":    countries,
		"judgeValidProxies":   entries,
	}

	if info.TopReputationProxy != nil {
		result["topReputationProxy"] = map[string]interface{}{
			"proxyId": int(info.TopReputationProxy.ProxyID),
			"ip":      info.TopReputationProxy.IP,
			"port":    int(info.TopReputationProxy.Port),
			"score":   info.TopReputationProxy.Score,
			"label":   info.TopReputationProxy.Label,
		}
	}

	return result
}
