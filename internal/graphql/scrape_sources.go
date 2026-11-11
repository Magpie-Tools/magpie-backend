package graphql

import "magpie/internal/scrapestore"

func buildScrapeSitePage(workspaceID uint, page int) map[string]interface{} {
	sites := scrapestore.GetScrapeSiteInfoPage(workspaceID, page)
	items := make([]map[string]interface{}, 0, len(sites))
	for _, site := range sites {
		items = append(items, map[string]interface{}{
			"id":         int(site.Id),
			"url":        site.Url,
			"proxyCount": int(site.ProxyCount),
			"addedAt":    site.AddedAt,
		})
	}

	return map[string]interface{}{
		"page":       page,
		"pageSize":   len(items),
		"totalCount": int(scrapestore.GetAllScrapeSiteCountOfUser(workspaceID)),
		"items":      items,
	}
}
