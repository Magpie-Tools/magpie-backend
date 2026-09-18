package dto

import "testing"

func TestNormalizeProxyStateFilter(t *testing.T) {
	for input, want := range map[string]string{"active": "active", " PAUSED ": "paused", "Archived": "archived", "all": "", "": "", "unknown": ""} {
		if got := NormalizeProxyStateFilter(input); got != want {
			t.Errorf("NormalizeProxyStateFilter(%q) = %q, want %q", input, got, want)
		}
	}
}
