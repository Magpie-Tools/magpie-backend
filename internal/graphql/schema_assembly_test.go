package graphql

import (
	"strings"
	"testing"

	gql "github.com/graphql-go/graphql"
)

// NewSchema assembles the Viewer type and the mutations from the feature
// files. Every feature's fields must reach the schema.
func TestNewSchemaAssemblesEveryFeature(t *testing.T) {
	schema, err := NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	viewer, ok := schema.Type("Viewer").(*gql.Object)
	if !ok {
		t.Fatal("schema has no Viewer object")
	}
	fields := viewer.Fields()
	for _, name := range []string{
		"id", "email", "role", "workspaceId", "workspaceName", "workspaceRole",
		"settings",
		"proxyCount", "proxyLimit", "proxies",
		"dashboard", "proxyHistory", "recentProxyChecks", "fastestAliveProxies", "proxySnapshots",
		"scrapeSourceUrls", "scrapeSourceCount", "scrapeSources",
	} {
		if fields[name] == nil {
			t.Errorf("Viewer.%s is missing", name)
		}
	}
	if len(fields) != 18 {
		t.Errorf("Viewer has %d fields, want 18", len(fields))
	}
	if schema.MutationType() == nil || schema.MutationType().Fields()["updateUserSettings"] == nil {
		t.Error("Mutation.updateUserSettings is missing")
	}
}

func TestMergeFieldsRejectsDuplicateFields(t *testing.T) {
	field := &gql.Field{Type: gql.String}
	if _, err := mergeFields("Viewer", gql.Fields{"id": field}, gql.Fields{"id": field}); err == nil || !strings.Contains(err.Error(), "Viewer.id") {
		t.Fatalf("duplicate field: %v", err)
	}
	merged, err := mergeFields("Viewer", gql.Fields{"a": field}, gql.Fields{"b": field})
	if err != nil || len(merged) != 2 {
		t.Fatalf("merge: %v %v", merged, err)
	}
}
