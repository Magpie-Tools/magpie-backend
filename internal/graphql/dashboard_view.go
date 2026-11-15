package graphql

import (
	"context"
	"errors"
	"sync"
	"time"

	"magpie/internal/dashboard"

	gql "github.com/graphql-go/graphql"
)

var errDashboardUnavailable = errors.New("graphql: dashboard service is not configured")

type requestScopeKey struct{}

// requestScope holds values resolved at most once per GraphQL request.
type requestScope struct {
	mu         sync.Mutex
	dashboards map[uint]*dashboardResolution
}

type dashboardResolution struct {
	once sync.Once
	view dashboard.View
	err  error
}

// WithRequestScope marks ctx as one GraphQL request. Fields that may wait,
// such as viewer.dashboardView, resolve once per request in it, so aliasing
// them cannot stack up waits.
func WithRequestScope(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestScopeKey{}, &requestScope{dashboards: make(map[uint]*dashboardResolution)})
}

// resolveDashboardView returns the workspace's dashboard view, resolved at
// most once per request scope. Without a scope it resolves directly.
func resolveDashboardView(ctx context.Context, views *dashboard.Service, workspaceID uint) (dashboard.View, error) {
	if views == nil {
		return dashboard.View{}, errDashboardUnavailable
	}
	scope, _ := ctx.Value(requestScopeKey{}).(*requestScope)
	if scope == nil {
		return views.View(ctx, workspaceID)
	}
	scope.mu.Lock()
	resolution := scope.dashboards[workspaceID]
	if resolution == nil {
		resolution = &dashboardResolution{}
		scope.dashboards[workspaceID] = resolution
	}
	scope.mu.Unlock()
	resolution.once.Do(func() {
		resolution.view, resolution.err = views.View(ctx, workspaceID)
	})
	return resolution.view, resolution.err
}

func newDashboardViewType(reputationType, countryType, judgeType, recentType, fastestType *gql.Object) *gql.Object {
	statusType := gql.NewEnum(gql.EnumConfig{
		Name:        "DashboardStatus",
		Description: "How current a dashboard view is.",
		Values: gql.EnumValueConfigMap{
			string(dashboard.StatusFresh):   &gql.EnumValueConfig{Value: dashboard.StatusFresh, Description: "Published less than 30 seconds ago or during this request."},
			string(dashboard.StatusStale):   &gql.EnumValueConfig{Value: dashboard.StatusStale, Description: "An older snapshot of up to ten minutes, or the last build failed and some part is still valid."},
			string(dashboard.StatusPending): &gql.EnumValueConfig{Value: dashboard.StatusPending, Description: "No valid snapshot yet; a build is pending. Absent parts are null."},
			string(dashboard.StatusFailed):  &gql.EnumValueConfig{Value: dashboard.StatusFailed, Description: "The last build failed and no part is valid."},
		},
	})
	topReputationType := gql.NewObject(gql.ObjectConfig{
		Name: "DashboardTopReputationProxy",
		Fields: gql.Fields{
			"proxyId": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"ip":      &gql.Field{Type: gql.NewNonNull(gql.String)},
			"port":    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"score":   &gql.Field{Type: gql.NewNonNull(gql.Float)},
			"label":   &gql.Field{Type: gql.NewNonNull(gql.String)},
		},
	})
	infoType := gql.NewObject(gql.ObjectConfig{
		Name:        "DashboardViewInfo",
		Description: "Dashboard figures. The two check figures are read live from the usage meter on every view; every other field comes from the snapshot and is null while the snapshot has no valid value for it.",
		Fields: gql.Fields{
			"totalChecks": &gql.Field{
				Type:        gql.NewNonNull(gql.Float),
				Description: "Check requests charged to this workspace since it was created: the larger of the metered and the ingested count of each month. A Float because the count exceeds the 32-bit GraphQL Int.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return float64(p.Source.(dashboard.View).TotalChecks), nil
				},
			},
			"checksThisMonth": &gql.Field{
				Type:        gql.NewNonNull(gql.Float),
				Description: "Check requests charged in the current monthly allowance period, as committed by the usage meter.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return float64(p.Source.(dashboard.View).ChecksThisMonth), nil
				},
			},
			"totalScraped": &gql.Field{
				Type: gql.Float,
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return optionalFloat(p.Source.(dashboard.View).TotalScraped()), nil
				},
			},
			"reputationBreakdown": &gql.Field{
				Type: reputationType,
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					breakdown := p.Source.(dashboard.View).ReputationBreakdown()
					if breakdown == nil {
						return nil, nil
					}
					return map[string]interface{}{"good": int(breakdown.Good), "neutral": int(breakdown.Neutral), "poor": int(breakdown.Poor), "unknown": int(breakdown.Unknown)}, nil
				},
			},
			"countryBreakdown": &gql.Field{
				Type: gql.NewList(gql.NewNonNull(countryType)),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					countries := p.Source.(dashboard.View).CountryBreakdown()
					if countries == nil {
						return nil, nil
					}
					result := make([]map[string]interface{}, 0, len(countries))
					for _, entry := range countries {
						result = append(result, map[string]interface{}{"country": entry.Country, "count": int(entry.Count)})
					}
					return result, nil
				},
			},
			"judgeValidProxies": &gql.Field{
				Type:        gql.NewList(gql.NewNonNull(judgeType)),
				Description: "Alive routes per judge and anonymity level. Outdated while healthOutdated; null before any health is built.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					judges := p.Source.(dashboard.View).JudgeValidProxies()
					if judges == nil {
						return nil, nil
					}
					result := make([]map[string]interface{}, 0, len(judges))
					for _, entry := range judges {
						result = append(result, map[string]interface{}{"judgeUrl": entry.JudgeURL, "eliteProxies": int(entry.EliteProxies), "anonymousProxies": int(entry.AnonymousProxies), "transparentProxies": int(entry.TransparentProxies)})
					}
					return result, nil
				},
			},
			"topReputationProxy": &gql.Field{
				Type: topReputationType,
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					top := p.Source.(dashboard.View).TopReputationProxy()
					if top == nil {
						return nil, nil
					}
					return map[string]interface{}{"proxyId": int(top.ProxyID), "ip": top.IP, "port": int(top.Port), "score": float64(top.Score), "label": top.Label}, nil
				},
			},
		},
	})
	return gql.NewObject(gql.ObjectConfig{
		Name:        "DashboardView",
		Description: "The workspace dashboard, served from a shared snapshot that any backend instance rebuilds on demand.",
		Fields: gql.Fields{
			"status": &gql.Field{
				Type: gql.NewNonNull(statusType),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return p.Source.(dashboard.View).Status, nil
				},
			},
			"rebuilding": &gql.Field{
				Type:        gql.NewNonNull(gql.Boolean),
				Description: "A build of this workspace's snapshot is running on some instance.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return p.Source.(dashboard.View).Rebuilding, nil
				},
			},
			"checkerSettingsPending": &gql.Field{
				Type:        gql.NewNonNull(gql.Boolean),
				Description: "The workspace's checker settings changed and are still being applied.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return p.Source.(dashboard.View).CheckerSettingsPending, nil
				},
			},
			"healthOutdated": &gql.Field{
				Type:        gql.NewNonNull(gql.Boolean),
				Description: "aliveCount, judgeValidProxies, recentProxyChecks and fastestAliveProxies are the previous result, computed under earlier checker settings, because results under the current settings are not built yet.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return p.Source.(dashboard.View).HealthOutdated, nil
				},
			},
			"healthCollectedAt": &gql.Field{
				Type:        gql.DateTime,
				Description: "When the build that computed aliveCount, judgeValidProxies, recentProxyChecks and fastestAliveProxies started. Earlier than collectedAt while healthOutdated.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return optionalTime(p.Source.(dashboard.View).HealthCollectedAt), nil
				},
			},
			"collectedAt": &gql.Field{
				Type:        gql.DateTime,
				Description: "When the snapshot's latest build started: its data is at least this old. It does not describe the live check figures, or the health fields while healthOutdated (see healthCollectedAt).",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return optionalTime(p.Source.(dashboard.View).CollectedAt), nil
				},
			},
			"computedAt": &gql.Field{
				Type:        gql.DateTime,
				Description: "When the snapshot was published.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return optionalTime(p.Source.(dashboard.View).ComputedAt), nil
				},
			},
			"info": &gql.Field{
				Type: gql.NewNonNull(infoType),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return p.Source, nil
				},
			},
			"recentProxyChecks": &gql.Field{
				Type:        gql.NewList(gql.NewNonNull(recentType)),
				Description: "The most recently checked routes, alive first. Outdated while healthOutdated; null before any health is built.",
				Args:        gql.FieldConfigArgument{"limit": &gql.ArgumentConfig{Type: gql.Int}},
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					limit := defaultRecentProxyChecksLimit
					if raw, ok := p.Args["limit"].(int); ok && raw > 0 {
						limit = clampPositiveLimit(raw, maxRecentProxyChecksLimit)
					}
					entries := p.Source.(dashboard.View).RecentProxyChecks(limit)
					if entries == nil {
						return nil, nil
					}
					return recentProxyCheckMaps(entries), nil
				},
			},
			"fastestAliveProxies": &gql.Field{
				Type:        gql.NewList(gql.NewNonNull(fastestType)),
				Description: "Alive routes by their best response time under the current checker settings, or under earlier ones while healthOutdated; null before any health is built.",
				Args:        gql.FieldConfigArgument{"limit": &gql.ArgumentConfig{Type: gql.Int}},
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					limit := defaultFastestAliveProxyLimit
					if raw, ok := p.Args["limit"].(int); ok && raw > 0 {
						limit = clampPositiveLimit(raw, maxFastestAliveProxyLimit)
					}
					entries := p.Source.(dashboard.View).FastestAliveProxies(limit)
					if entries == nil {
						return nil, nil
					}
					return fastestAliveProxyMaps(entries), nil
				},
			},
			"aliveCount": &gql.Field{
				Type:        gql.Float,
				Description: "Alive routes under the current checker settings, or under earlier ones while healthOutdated; null before any health is built.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return optionalFloat(p.Source.(dashboard.View).AliveCount()), nil
				},
			},
			"proxyCount": &gql.Field{
				Type:        gql.Float,
				Description: "Routes the workspace owns, in any state.",
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return optionalFloat(p.Source.(dashboard.View).ProxyCount()), nil
				},
			},
		},
	})
}

func optionalFloat(value *int64) interface{} {
	if value == nil {
		return nil
	}
	return float64(*value)
}

func optionalTime(value *time.Time) interface{} {
	if value == nil {
		return nil
	}
	return *value
}
