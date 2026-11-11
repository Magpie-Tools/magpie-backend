package graphql

import (
	"context"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"

	gql "github.com/graphql-go/graphql"
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

// dashboardTypes holds the GraphQL types of the dashboard fields of Viewer.
type dashboardTypes struct {
	dashboardType               *gql.Object
	fastestAliveProxyType       *gql.Object
	proxyHistoryType            *gql.Object
	proxySnapshotCollectionType *gql.Object
	recentProxyCheckType        *gql.Object
}

func newDashboardTypes() dashboardTypes {

	dashboardReputationBreakdownType := gql.NewObject(gql.ObjectConfig{
		Name: "DashboardReputationBreakdown",
		Fields: gql.Fields{
			"good":    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"neutral": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"poor":    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"unknown": &gql.Field{Type: gql.NewNonNull(gql.Int)},
		},
	})

	proxyHistoryType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxyHistoryEntry",
		Fields: gql.Fields{
			"count":      &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"recordedAt": &gql.Field{Type: gql.NewNonNull(gql.DateTime)},
		},
	})

	recentProxyCheckType := gql.NewObject(gql.ObjectConfig{
		Name: "RecentProxyCheck",
		Fields: gql.Fields{
			"id":           &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"ip":           &gql.Field{Type: gql.NewNonNull(gql.String)},
			"port":         &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"responseTime": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"alive":        &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"latestCheck":  &gql.Field{Type: gql.DateTime},
		},
	})

	fastestAliveProxyType := gql.NewObject(gql.ObjectConfig{
		Name: "FastestAliveProxy",
		Fields: gql.Fields{
			"id":              &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"ip":              &gql.Field{Type: gql.NewNonNull(gql.String)},
			"port":            &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"responseTime":    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"country":         &gql.Field{Type: gql.NewNonNull(gql.String)},
			"reputationLabel": &gql.Field{Type: gql.NewNonNull(gql.String)},
			"reputationScore": &gql.Field{Type: gql.NewNonNull(gql.Float)},
			"latestCheck":     &gql.Field{Type: gql.DateTime},
		},
	})

	proxySnapshotType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxySnapshotEntry",
		Fields: gql.Fields{
			"count":      &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"recordedAt": &gql.Field{Type: gql.NewNonNull(gql.DateTime)},
		},
	})

	proxySnapshotCollectionType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxySnapshotCollection",
		Fields: gql.Fields{
			"alive": &gql.Field{
				Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(proxySnapshotType))),
			},
			"scraped": &gql.Field{
				Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(proxySnapshotType))),
			},
		},
	})

	judgeValidProxyType := gql.NewObject(gql.ObjectConfig{
		Name: "JudgeValidProxy",
		Fields: gql.Fields{
			"judgeUrl":           &gql.Field{Type: gql.NewNonNull(gql.String)},
			"eliteProxies":       &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"anonymousProxies":   &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"transparentProxies": &gql.Field{Type: gql.NewNonNull(gql.Int)},
		},
	})

	countryBreakdownType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxyCountryBreakdown",
		Fields: gql.Fields{
			"country": &gql.Field{Type: gql.NewNonNull(gql.String)},
			"count":   &gql.Field{Type: gql.NewNonNull(gql.Int)},
		},
	})

	dashboardType := gql.NewObject(gql.ObjectConfig{
		Name: "DashboardInfo",
		Fields: gql.Fields{
			"totalChecks":      &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"totalScraped":     &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"totalChecksWeek":  &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"totalScrapedWeek": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"reputationBreakdown": &gql.Field{
				Type: gql.NewNonNull(dashboardReputationBreakdownType),
			},
			"countryBreakdown": &gql.Field{
				Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(countryBreakdownType))),
			},
			"judgeValidProxies": &gql.Field{
				Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(judgeValidProxyType))),
			},
		},
	})
	return dashboardTypes{dashboardType: dashboardType, fastestAliveProxyType: fastestAliveProxyType, proxyHistoryType: proxyHistoryType, proxySnapshotCollectionType: proxySnapshotCollectionType, recentProxyCheckType: recentProxyCheckType}
}

// dashboardViewerFields are the fields this feature adds to the Viewer type.
func dashboardViewerFields(types dashboardTypes) gql.Fields {
	dashboardType, fastestAliveProxyType, proxyHistoryType, proxySnapshotCollectionType, recentProxyCheckType := types.dashboardType, types.fastestAliveProxyType, types.proxyHistoryType, types.proxySnapshotCollectionType, types.recentProxyCheckType
	return gql.Fields{
		"dashboard": &gql.Field{
			Type: gql.NewNonNull(dashboardType),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					info, err := database.GetDashboardInfoContext(p.Context, data.workspace.ID)
					if err != nil {
						return nil, err
					}
					return buildDashboard(info), nil
				}
				return nil, nil
			},
		},
		"proxyHistory": &gql.Field{
			Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(proxyHistoryType))),
			Args: gql.FieldConfigArgument{
				"limit": &gql.ArgumentConfig{Type: gql.Int},
			},
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				limit := defaultViewerProxyHistoryLimit
				if raw, ok := p.Args["limit"].(int); ok && raw > 0 {
					limit = clampPositiveLimit(raw, maxViewerProxyHistoryLimit)
				}
				if data, ok := p.Source.(*viewerData); ok {
					return buildProxyHistory(data.workspace.ID, limit), nil
				}
				return []map[string]interface{}{}, nil
			},
		},
		"recentProxyChecks": &gql.Field{
			Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(recentProxyCheckType))),
			Args: gql.FieldConfigArgument{
				"limit": &gql.ArgumentConfig{Type: gql.Int},
			},
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				limit := defaultRecentProxyChecksLimit
				if raw, ok := p.Args["limit"].(int); ok && raw > 0 {
					limit = clampPositiveLimit(raw, maxRecentProxyChecksLimit)
				}
				if data, ok := p.Source.(*viewerData); ok {
					return buildRecentProxyChecksContext(p.Context, data.workspace.ID, limit)
				}
				return []map[string]interface{}{}, nil
			},
		},
		"fastestAliveProxies": &gql.Field{
			Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(fastestAliveProxyType))),
			Args: gql.FieldConfigArgument{
				"limit": &gql.ArgumentConfig{Type: gql.Int},
			},
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				limit := defaultFastestAliveProxyLimit
				if raw, ok := p.Args["limit"].(int); ok && raw > 0 {
					limit = clampPositiveLimit(raw, maxFastestAliveProxyLimit)
				}
				if data, ok := p.Source.(*viewerData); ok {
					return buildFastestAliveProxiesContext(p.Context, data.workspace.ID, limit)
				}
				return []map[string]interface{}{}, nil
			},
		},
		"proxySnapshots": &gql.Field{
			Type: gql.NewNonNull(proxySnapshotCollectionType),
			Args: gql.FieldConfigArgument{
				"limit": &gql.ArgumentConfig{Type: gql.Int},
			},
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				limit := 0
				if raw, ok := p.Args["limit"].(int); ok && raw > 0 {
					limit = clampPositiveLimit(raw, maxViewerProxySnapshotLimit)
				}
				if data, ok := p.Source.(*viewerData); ok {
					alive := database.GetProxySnapshotEntries(data.workspace.ID, domain.ProxySnapshotMetricAlive, limit)
					alive = ensureLatestAliveSnapshot(alive, database.GetCurrentAliveProxyCount(data.workspace.ID))

					scraped := database.GetProxySnapshotEntries(data.workspace.ID, domain.ProxySnapshotMetricScraped, limit)
					return map[string]interface{}{
						"alive":   buildProxySnapshots(alive),
						"scraped": buildProxySnapshots(scraped),
					}, nil
				}
				return map[string]interface{}{
					"alive":   []map[string]interface{}{},
					"scraped": []map[string]interface{}{},
				}, nil
			},
		},
	}
}
