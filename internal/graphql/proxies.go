package graphql

import (
	"sort"

	"magpie/internal/api/dto"
	"magpie/internal/database"
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
