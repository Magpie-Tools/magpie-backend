package config

import (
	"reflect"
	"testing"
)

func TestNormalizeWebsiteBlacklist(t *testing.T) {
	input := []string{" Example.com ", "http://Example.com/path", "sub.example.com", "https://sub.example.com"}
	want := []string{"example.com", "sub.example.com"}

	got := NormalizeWebsiteBlacklist(input)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeWebsiteBlacklist(%v) = %v, want %v", input, got, want)
	}
}

func TestIsWebsiteBlocked(t *testing.T) {
	updateWebsiteBlocklist([]string{"example.com"})
	defer updateWebsiteBlocklist(nil)

	cases := []struct {
		url      string
		blocked  bool
		testName string
	}{
		{"http://example.com", true, "exact host"},
		{"https://api.example.com/resource", true, "subdomain"},
		{"https://example.net", false, "different domain"},
	}

	for _, tc := range cases {
		if got := IsWebsiteBlocked(tc.url); got != tc.blocked {
			t.Errorf("%s: IsWebsiteBlocked(%q) = %v, want %v", tc.testName, tc.url, got, tc.blocked)
		}
	}
}
