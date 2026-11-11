package graphql

import (
	"fmt"

	gql "github.com/graphql-go/graphql"

	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/judgestore"
	"magpie/internal/scrapestore"
)

func NewSchema() (gql.Schema, error) {
	// Retain the historical GraphQL type name for workspace judge settings.
	simpleJudgeType := gql.NewObject(gql.ObjectConfig{
		Name: "SimpleUserJudge",
		Fields: gql.Fields{
			"url":   &gql.Field{Type: gql.NewNonNull(gql.String)},
			"regex": &gql.Field{Type: gql.NewNonNull(gql.String)},
		},
	})

	checkerSettingsType, checkerSettingsInput := checkerSettingsTypes()
	userSettingsType := gql.NewObject(gql.ObjectConfig{
		Name: "UserSettings",
		Fields: gql.Fields{
			"checkerSettings":            &gql.Field{Type: checkerSettingsType},
			"transportProtocol":          &gql.Field{Type: gql.String},
			"httpProtocol":               &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"httpsProtocol":              &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"socks4Protocol":             &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"socks5Protocol":             &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"timeout":                    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"retries":                    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"useHttpsForSocks":           &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"autoRemoveFailingProxies":   &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"autoRemoveFailureThreshold": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"failureAction":              &gql.Field{Type: gql.NewNonNull(gql.String)},
			"judges":                     &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(simpleJudgeType)))},
			"scrapingSources":            &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String)))},
			"proxyListColumns":           &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String)))},
			"scrapeSourceProxyColumns":   &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String)))},
			"scrapeSourceListColumns":    &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String)))},
		},
	})

	proxyReputationType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxyReputation",
		Fields: gql.Fields{
			"kind":  &gql.Field{Type: gql.NewNonNull(gql.String)},
			"score": &gql.Field{Type: gql.NewNonNull(gql.Float)},
			"label": &gql.Field{Type: gql.NewNonNull(gql.String)},
		},
	})

	proxyReputationSummaryType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxyReputationSummary",
		Fields: gql.Fields{
			"overall": &gql.Field{Type: proxyReputationType},
			"protocols": &gql.Field{
				Type: gql.NewList(gql.NewNonNull(proxyReputationType)),
			},
		},
	})

	proxyTagType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxyTag",
		Fields: gql.Fields{
			"id":    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"name":  &gql.Field{Type: gql.NewNonNull(gql.String)},
			"color": &gql.Field{Type: gql.NewNonNull(gql.String)},
		},
	})

	dashboardReputationBreakdownType := gql.NewObject(gql.ObjectConfig{
		Name: "DashboardReputationBreakdown",
		Fields: gql.Fields{
			"good":    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"neutral": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"poor":    &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"unknown": &gql.Field{Type: gql.NewNonNull(gql.Int)},
		},
	})

	proxyType := gql.NewObject(gql.ObjectConfig{
		Name: "Proxy",
		Fields: gql.Fields{
			"id":             &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"state":          &gql.Field{Type: gql.NewNonNull(gql.String)},
			"pauseReason":    &gql.Field{Type: gql.String},
			"ip":             &gql.Field{Type: gql.NewNonNull(gql.String)},
			"port":           &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"estimatedType":  &gql.Field{Type: gql.NewNonNull(gql.String)},
			"responseTime":   &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"country":        &gql.Field{Type: gql.NewNonNull(gql.String)},
			"anonymityLevel": &gql.Field{Type: gql.NewNonNull(gql.String)},
			"protocol":       &gql.Field{Type: gql.NewNonNull(gql.String)},
			"alive":          &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"healthKnown":    &gql.Field{Type: gql.NewNonNull(gql.Boolean)},
			"latestCheck":    &gql.Field{Type: gql.DateTime},
			"reputation":     &gql.Field{Type: proxyReputationSummaryType},
			"tags":           &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(proxyTagType)))},
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

	proxyPageType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxyPage",
		Fields: gql.Fields{
			"page":       &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"pageSize":   &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"totalCount": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"items":      &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(proxyType)))},
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

	scrapeSiteType := gql.NewObject(gql.ObjectConfig{
		Name: "ScrapeSite",
		Fields: gql.Fields{
			"id":         &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"url":        &gql.Field{Type: gql.NewNonNull(gql.String)},
			"proxyCount": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"addedAt":    &gql.Field{Type: gql.DateTime},
		},
	})

	scrapeSitePageType := gql.NewObject(gql.ObjectConfig{
		Name: "ScrapeSitePage",
		Fields: gql.Fields{
			"page":       &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"pageSize":   &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"totalCount": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"items":      &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(scrapeSiteType)))},
		},
	})

	viewerType := gql.NewObject(gql.ObjectConfig{
		Name: "Viewer",
		Fields: gql.Fields{
			"id": &gql.Field{
				Type: gql.NewNonNull(gql.ID),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return fmt.Sprintf("%d", data.user.ID), nil
					}
					return nil, nil
				},
			},
			"email": &gql.Field{
				Type: gql.NewNonNull(gql.String),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return data.user.Email, nil
					}
					return nil, nil
				},
			},
			"role": &gql.Field{
				Type: gql.NewNonNull(gql.String),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return data.user.Role, nil
					}
					return nil, nil
				},
			},
			"workspaceId": &gql.Field{
				Type: gql.NewNonNull(gql.ID),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return fmt.Sprintf("%d", data.workspace.ID), nil
					}
					return nil, nil
				},
			},
			"workspaceName": &gql.Field{
				Type: gql.NewNonNull(gql.String),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return data.workspace.Name, nil
					}
					return nil, nil
				},
			},
			"workspaceRole": &gql.Field{
				Type: gql.NewNonNull(gql.String),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return data.role, nil
					}
					return nil, nil
				},
			},
			"settings": &gql.Field{
				Type: gql.NewNonNull(userSettingsType),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						judges := judgestore.GetWorkspaceJudges(data.workspace.ID)
						sources := scrapestore.GetScrapingSourcesOfUsers(data.workspace.ID)
						return buildUserSettings(data.workspace, data.preference, judges, sources), nil
					}
					return nil, nil
				},
			},
			"scrapeSourceUrls": &gql.Field{
				Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return scrapestore.GetScrapingSourcesOfUsers(data.workspace.ID), nil
					}
					return []string{}, nil
				},
			},
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
			"proxyCount": &gql.Field{
				Type: gql.NewNonNull(gql.Int),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return int(database.GetWorkspaceProxyCount(data.workspace.ID)), nil
					}
					return 0, nil
				},
			},
			"proxyLimit": &gql.Field{
				Type: gql.Int,
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						limit, unlimited := data.workspace.Subscription.ActivationLimit()
						if unlimited {
							return nil, nil
						}
						return int(limit), nil
					}
					return nil, nil
				},
			},
			"proxies": &gql.Field{
				Type: gql.NewNonNull(proxyPageType),
				Args: gql.FieldConfigArgument{
					"page": &gql.ArgumentConfig{Type: gql.NewNonNull(gql.Int)},
				},
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					page := 1
					if raw, ok := p.Args["page"].(int); ok && raw > 0 {
						page = raw
					}
					if data, ok := p.Source.(*viewerData); ok {
						return buildProxyPage(data.workspace.ID, page), nil
					}
					return nil, nil
				},
			},
			"scrapeSourceCount": &gql.Field{
				Type: gql.NewNonNull(gql.Int),
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					if data, ok := p.Source.(*viewerData); ok {
						return int(scrapestore.GetAllScrapeSiteCountOfUser(data.workspace.ID)), nil
					}
					return 0, nil
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
			"scrapeSources": &gql.Field{
				Type: gql.NewNonNull(scrapeSitePageType),
				Args: gql.FieldConfigArgument{
					"page": &gql.ArgumentConfig{Type: gql.NewNonNull(gql.Int)},
				},
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					page := 1
					if raw, ok := p.Args["page"].(int); ok && raw > 0 {
						page = raw
					}
					if data, ok := p.Source.(*viewerData); ok {
						return buildScrapeSitePage(data.workspace.ID, page), nil
					}
					return nil, nil
				},
			},
		},
	})

	queryType := gql.NewObject(gql.ObjectConfig{
		Name: "Query",
		Fields: gql.Fields{
			"viewer": &gql.Field{
				Type: viewerType,
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					return fetchViewer(p.Context)
				},
			},
		},
	})

	judgeInputType := gql.NewInputObject(gql.InputObjectConfig{
		Name: "SimpleUserJudgeInput",
		Fields: gql.InputObjectConfigFieldMap{
			"url":   &gql.InputObjectFieldConfig{Type: gql.NewNonNull(gql.String)},
			"regex": &gql.InputObjectFieldConfig{Type: gql.NewNonNull(gql.String)},
		},
	})

	updateSettingsInput := gql.NewInputObject(gql.InputObjectConfig{
		Name: "UpdateUserSettingsInput",
		Fields: gql.InputObjectConfigFieldMap{
			"checkerSettings":            &gql.InputObjectFieldConfig{Type: checkerSettingsInput},
			"transportProtocol":          &gql.InputObjectFieldConfig{Type: gql.String},
			"httpProtocol":               &gql.InputObjectFieldConfig{Type: gql.Boolean},
			"httpsProtocol":              &gql.InputObjectFieldConfig{Type: gql.Boolean},
			"socks4Protocol":             &gql.InputObjectFieldConfig{Type: gql.Boolean},
			"socks5Protocol":             &gql.InputObjectFieldConfig{Type: gql.Boolean},
			"timeout":                    &gql.InputObjectFieldConfig{Type: gql.Int},
			"retries":                    &gql.InputObjectFieldConfig{Type: gql.Int},
			"useHttpsForSocks":           &gql.InputObjectFieldConfig{Type: gql.Boolean},
			"autoRemoveFailingProxies":   &gql.InputObjectFieldConfig{Type: gql.Boolean},
			"autoRemoveFailureThreshold": &gql.InputObjectFieldConfig{Type: gql.Int},
			"failureAction":              &gql.InputObjectFieldConfig{Type: gql.String},
			"judges": &gql.InputObjectFieldConfig{
				Type: gql.NewList(gql.NewNonNull(judgeInputType)),
			},
			"proxyListColumns": &gql.InputObjectFieldConfig{
				Type: gql.NewList(gql.NewNonNull(gql.String)),
			},
			"scrapeSourceProxyColumns": &gql.InputObjectFieldConfig{
				Type: gql.NewList(gql.NewNonNull(gql.String)),
			},
			"scrapeSourceListColumns": &gql.InputObjectFieldConfig{
				Type: gql.NewList(gql.NewNonNull(gql.String)),
			},
		},
	})

	mutationType := gql.NewObject(gql.ObjectConfig{
		Name: "Mutation",
		Fields: gql.Fields{
			"updateUserSettings": &gql.Field{
				Type: userSettingsType,
				Args: gql.FieldConfigArgument{
					"input": &gql.ArgumentConfig{Type: gql.NewNonNull(updateSettingsInput)},
				},
				Resolve: func(p gql.ResolveParams) (interface{}, error) {
					rawInput, _ := p.Args["input"].(map[string]interface{})
					if err := applyUserSettings(p.Context, rawInput); err != nil {
						return nil, err
					}
					viewer, err := fetchViewer(p.Context)
					if err != nil {
						return nil, err
					}
					if data, ok := viewer.(*viewerData); ok {
						judges := judgestore.GetWorkspaceJudges(data.workspace.ID)
						sources := scrapestore.GetScrapingSourcesOfUsers(data.workspace.ID)
						return buildUserSettings(data.workspace, data.preference, judges, sources), nil
					}
					return nil, nil
				},
			},
		},
	})

	return gql.NewSchema(gql.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	})
}
