package graphql

import (
	"fmt"

	gql "github.com/graphql-go/graphql"
)

// NewSchema assembles the GraphQL schema. Each feature file defines its types
// and the fields it adds to the Viewer type and to mutations; this function
// only wires them together.
func NewSchema() (gql.Schema, error) {
	settings := newSettingsTypes()
	proxies := newProxyTypes()
	dashboard := newDashboardTypes()
	sources := newScrapeSourceTypes()

	viewerFields, err := mergeFields("Viewer",
		viewerIdentityFields(),
		settingsViewerFields(settings),
		proxyViewerFields(proxies),
		dashboardViewerFields(dashboard),
		scrapeSourceViewerFields(sources),
	)
	if err != nil {
		return gql.Schema{}, err
	}
	viewerType := gql.NewObject(gql.ObjectConfig{
		Name:   "Viewer",
		Fields: viewerFields,
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

	mutationType := gql.NewObject(gql.ObjectConfig{
		Name:   "Mutation",
		Fields: settingsMutationFields(settings),
	})

	return gql.NewSchema(gql.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	})
}

// mergeFields combines the fields that features add to one type. A field
// defined by two features is a programming error.
func mergeFields(typeName string, groups ...gql.Fields) (gql.Fields, error) {
	merged := gql.Fields{}
	for _, group := range groups {
		for name, field := range group {
			if _, exists := merged[name]; exists {
				return nil, fmt.Errorf("graphql: %s.%s is defined twice", typeName, name)
			}
			merged[name] = field
		}
	}
	return merged, nil
}
