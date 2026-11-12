package graphql

import (
	"sort"

	"magpie/internal/api/dto"
	"magpie/internal/database"

	gql "github.com/graphql-go/graphql"
)

func buildProxyPage(workspaceID uint, page int) map[string]interface{} {
	proxies, total := database.GetProxyInfoPageWithCount(workspaceID, page)
	if total == 0 {
		// A failed projection read also returns zero. Keep the independent
		// ownership total that this endpoint exposed before count reuse.
		total = database.GetWorkspaceProxyCount(workspaceID)
	}
	items := make([]map[string]interface{}, 0, len(proxies))
	for _, proxy := range proxies {
		tags := make([]map[string]interface{}, 0, len(proxy.Tags))
		for _, tag := range proxy.Tags {
			tags = append(tags, map[string]interface{}{
				"id": int(tag.ID), "name": tag.Name, "color": tag.Color,
			})
		}
		items = append(items, map[string]interface{}{
			"id":             proxy.Id,
			"state":          proxy.State,
			"pauseReason":    proxy.PauseReason,
			"ip":             proxy.IP,
			"port":           int(proxy.Port),
			"estimatedType":  proxy.EstimatedType,
			"responseTime":   int(proxy.ResponseTime),
			"country":        proxy.Country,
			"anonymityLevel": proxy.AnonymityLevel,
			"protocol":       "",
			"alive":          proxy.Alive,
			"healthKnown":    proxy.HealthKnown,
			"latestCheck":    proxy.LatestCheck,
			"reputation":     buildGraphQLReputationSummary(proxy.Reputation),
			"tags":           tags,
		})
	}

	return map[string]interface{}{
		"page":       page,
		"pageSize":   len(items),
		"totalCount": int(total),
		"items":      items,
	}
}

func buildGraphQLReputationSummary(summary *dto.ProxyReputationSummary) interface{} {
	if summary == nil {
		return nil
	}

	result := make(map[string]interface{})

	if summary.Overall != nil {
		result["overall"] = graphQLReputationEntry(*summary.Overall)
	}

	if len(summary.Protocols) > 0 {
		keys := make([]string, 0, len(summary.Protocols))
		for key := range summary.Protocols {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		protocols := make([]map[string]interface{}, 0, len(keys))
		for _, key := range keys {
			rep := summary.Protocols[key]
			protocols = append(protocols, graphQLReputationEntry(rep))
		}

		result["protocols"] = protocols
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func graphQLReputationEntry(rep dto.ProxyReputation) map[string]interface{} {
	return map[string]interface{}{
		"kind":  rep.Kind,
		"score": rep.Score,
		"label": rep.Label,
	}
}

// proxyTypes holds the GraphQL types of the route fields of Viewer.
type proxyTypes struct {
	proxyPageType *gql.Object
}

func newProxyTypes() proxyTypes {

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

	proxyPageType := gql.NewObject(gql.ObjectConfig{
		Name: "ProxyPage",
		Fields: gql.Fields{
			"page":       &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"pageSize":   &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"totalCount": &gql.Field{Type: gql.NewNonNull(gql.Int)},
			"items":      &gql.Field{Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(proxyType)))},
		},
	})
	return proxyTypes{proxyPageType: proxyPageType}
}

// proxyViewerFields are the fields this feature adds to the Viewer type.
func proxyViewerFields(types proxyTypes) gql.Fields {
	proxyPageType := types.proxyPageType
	return gql.Fields{
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
	}
}
