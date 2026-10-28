package database

import (
	"reflect"
	"testing"

	"magpie/internal/domain"
)

func TestProxyFilterOptionsPreserveExactValuesAndLifecycleScope(t *testing.T) {
	db := setupProxyTagTestDB(t)
	rows := []domain.WorkspaceProxyFilterIndex{
		{WorkspaceID: 1, ProxyID: 1, State: "active", Country: "DE", EstimatedType: "residential", AnonymityLevel: "elite"},
		{WorkspaceID: 1, ProxyID: 2, State: "paused", Country: "de", EstimatedType: "Residential", AnonymityLevel: "Elite"},
		{WorkspaceID: 1, ProxyID: 3, State: "archived", Country: "US", EstimatedType: "mobile", AnonymityLevel: "transparent"},
		{WorkspaceID: 1, ProxyID: 4, Country: "", EstimatedType: "", AnonymityLevel: ""},
		{WorkspaceID: 1, ProxyID: 5, Country: " ", EstimatedType: "  ", AnonymityLevel: "\t"},
		{WorkspaceID: 1, ProxyID: 6, Country: "DE", EstimatedType: "residential", AnonymityLevel: "elite"},
		{WorkspaceID: 2, ProxyID: 1, Country: "other-owner", EstimatedType: "other-type", AnonymityLevel: "other-level"},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	// Empty strings and SQL NULL both used to produce N/A; whitespace-only
	// display values are excluded after the database orders the distinct values.
	if err := db.Exec(`UPDATE user_proxy_filter_indexes SET country='',estimated_type='',anonymity_level='' WHERE workspace_id=1 AND proxy_id=4`).Error; err != nil {
		t.Fatal(err)
	}
	options, err := GetProxyFilterOptions(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(options.Countries, []string{"DE", "N/A", "US", "de"}) ||
		!reflect.DeepEqual(options.Types, []string{"N/A", "Residential", "mobile", "residential"}) ||
		!reflect.DeepEqual(options.AnonymityLevels, []string{"Elite", "N/A", "elite", "transparent"}) {
		t.Fatal("filter values changed", options)
	}
	empty, err := GetProxyFilterOptions(100)
	if err != nil || empty.Countries == nil || len(empty.Countries) != 0 || empty.Types == nil || len(empty.Types) != 0 || empty.AnonymityLevels == nil || len(empty.AnonymityLevels) != 0 {
		t.Fatalf("empty workspace options=%+v error=%v", empty, err)
	}
	if err := db.Migrator().DropTable(&domain.WorkspaceProxyFilterIndex{}); err != nil {
		t.Fatal(err)
	}
	if _, err := GetProxyFilterOptions(1); err == nil {
		t.Fatal("option query error was hidden")
	}
}
