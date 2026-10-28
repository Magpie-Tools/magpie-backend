package graphql

import (
	"context"
	"errors"
	"testing"

	gql "github.com/graphql-go/graphql"
	"magpie/internal/domain"
)

func TestDashboardResolversPassCanceledRequestContext(t *testing.T) {
	schema, err := NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	viewer := schema.QueryType().Fields()["viewer"].Type.(*gql.Object)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, field := range []string{"dashboard", "recentProxyChecks", "fastestAliveProxies"} {
		t.Run(field, func(t *testing.T) {
			_, err := viewer.Fields()[field].Resolve(gql.ResolveParams{Context: ctx, Source: &viewerData{workspace: domain.Workspace{ID: 1}}, Args: map[string]interface{}{}})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("resolver discarded context cancellation: %v", err)
			}
		})
	}
}
