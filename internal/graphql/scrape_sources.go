package graphql

import (
	"magpie/internal/scrapestore"

	gql "github.com/graphql-go/graphql"
)

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

// scrapeSourceTypes holds the GraphQL types of the scrape-source fields of
// Viewer.
type scrapeSourceTypes struct {
	scrapeSitePageType *gql.Object
}

func newScrapeSourceTypes() scrapeSourceTypes {

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
	return scrapeSourceTypes{scrapeSitePageType: scrapeSitePageType}
}

// scrapeSourceViewerFields are the fields this feature adds to the Viewer type.
func scrapeSourceViewerFields(types scrapeSourceTypes) gql.Fields {
	scrapeSitePageType := types.scrapeSitePageType
	return gql.Fields{
		"scrapeSourceUrls": &gql.Field{
			Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					return scrapestore.GetScrapingSourcesOfUsers(data.workspace.ID), nil
				}
				return []string{}, nil
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
	}
}
