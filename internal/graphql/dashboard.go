package graphql

import (
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/dashboard"
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

func recentProxyCheckMaps(entries []dto.ProxyRecentCheck) []map[string]interface{} {
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
	return result
}

func fastestAliveProxyMaps(entries []dto.ProxyFastestAlive) []map[string]interface{} {
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
	return result
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

// ensureLatestAliveSnapshot appends the dashboard snapshot's alive count at
// the time the build that computed it started, unless the series already has
// a point at least that recent. An equal count moves the last point forward
// instead.
func ensureLatestAliveSnapshot(entries []dto.ProxySnapshotEntry, currentCount int64, recordedAt time.Time) []dto.ProxySnapshotEntry {
	if len(entries) == 0 {
		return append(entries, dto.ProxySnapshotEntry{Count: currentCount, RecordedAt: recordedAt})
	}
	lastIndex := len(entries) - 1
	if !recordedAt.After(entries[lastIndex].RecordedAt) {
		return entries
	}
	if entries[lastIndex].Count == currentCount {
		entries[lastIndex].RecordedAt = recordedAt
		return entries
	}
	return append(entries, dto.ProxySnapshotEntry{Count: currentCount, RecordedAt: recordedAt})
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

// dashboardTypes holds the GraphQL types of the dashboard fields of Viewer.
type dashboardTypes struct {
	dashboardViewType           *gql.Object
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

	dashboardViewType := newDashboardViewType(dashboardReputationBreakdownType, countryBreakdownType, judgeValidProxyType, recentProxyCheckType, fastestAliveProxyType)
	return dashboardTypes{dashboardViewType: dashboardViewType, fastestAliveProxyType: fastestAliveProxyType, proxyHistoryType: proxyHistoryType, proxySnapshotCollectionType: proxySnapshotCollectionType, recentProxyCheckType: recentProxyCheckType}
}

// dashboardViewerFields are the fields this feature adds to the Viewer type.
func dashboardViewerFields(types dashboardTypes, views *dashboard.Service) gql.Fields {
	proxyHistoryType, proxySnapshotCollectionType := types.proxyHistoryType, types.proxySnapshotCollectionType
	return gql.Fields{
		"dashboardView": &gql.Field{
			Type:        gql.NewNonNull(types.dashboardViewType),
			Description: "The workspace dashboard from its shared snapshot, with an explicit status.",
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				data, ok := p.Source.(*viewerData)
				if !ok {
					return nil, nil
				}
				return resolveDashboardView(p.Context, views, data.workspace.ID)
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
					// The latest point is the dashboard snapshot's alive count,
					// resolved once per request with dashboardView and stamped
					// with the start of the build that computed it, so an
					// outdated count lands at its own time. No live alive
					// aggregate runs here; without health the series gets no
					// synthetic point rather than a zero.
					view, err := resolveDashboardView(p.Context, views, data.workspace.ID)
					if err != nil {
						return nil, err
					}
					alive := database.GetProxySnapshotEntries(data.workspace.ID, domain.ProxySnapshotMetricAlive, limit)
					if count := view.AliveCount(); count != nil && view.HealthCollectedAt != nil {
						alive = ensureLatestAliveSnapshot(alive, *count, *view.HealthCollectedAt)
					}

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
