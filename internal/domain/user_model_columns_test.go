package domain

import (
	"slices"
	"testing"
)

func TestNormalizeProxyListColumns_PreservesCheckNow(t *testing.T) {
	columns := NormalizeProxyListColumns([]string{"alive", "check_now", "actions"})

	if len(columns) != 3 {
		t.Fatalf("len(columns) = %d, want 3", len(columns))
	}
	if columns[1] != "check_now" {
		t.Fatalf("columns[1] = %q, want check_now", columns[1])
	}
}

func TestNormalizeProxyListColumns_PreservesTags(t *testing.T) {
	columns := NormalizeProxyListColumns([]string{"alive", "ip_port", "tags", "actions"})

	if len(columns) != 4 {
		t.Fatalf("len(columns) = %d, want 4", len(columns))
	}
	if columns[2] != "tags" {
		t.Fatalf("columns[2] = %q, want tags", columns[2])
	}
}

func TestNormalizeScrapeSourceListColumns_PreservesScrapeNow(t *testing.T) {
	columns := NormalizeScrapeSourceListColumns([]string{"url", "scrape_now", "actions"})

	if len(columns) != 3 {
		t.Fatalf("len(columns) = %d, want 3", len(columns))
	}
	if columns[1] != "scrape_now" {
		t.Fatalf("columns[1] = %q, want scrape_now", columns[1])
	}
}

func TestNormalizeScrapeSourceListColumns_PreservesAliveCount(t *testing.T) {
	columns := NormalizeScrapeSourceListColumns([]string{"url", "alive_count", "actions"})

	if len(columns) != 3 {
		t.Fatalf("len(columns) = %d, want 3", len(columns))
	}
	if columns[1] != "alive_count" {
		t.Fatalf("columns[1] = %q, want alive_count", columns[1])
	}
}

func TestNormalizeColumnsPreservesAliasesOrderAndIndependentDefaults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		normalize   func([]string) []string
		input, want []string
	}{
		{"proxy", NormalizeProxyListColumns, []string{"alive_ratio_http", "invalid", "health_http", "tags"}, []string{"health_http", "tags"}},
		{"source proxy", NormalizeScrapeSourceProxyColumns, []string{"alive_ratio_http", "invalid", "health_http", "tags"}, []string{"health_http", "tags"}},
		{"source", NormalizeScrapeSourceListColumns, []string{"details", "invalid", "actions", "url"}, []string{"actions", "url"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.normalize(tc.input)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("normalized columns = %v, want %v", got, tc.want)
			}
			defaults := tc.normalize(nil)
			fallback := tc.normalize([]string{"invalid"})
			if !slices.Equal(fallback, defaults) {
				t.Fatalf("invalid columns fallback = %v, want %v", fallback, defaults)
			}
			fallback[0] = "modified"
			if !slices.Equal(tc.normalize(nil), defaults) {
				t.Fatal("caller mutated shared defaults")
			}
		})
	}
}
