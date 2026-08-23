package blacklist

import (
	"testing"

	"magpie/internal/domain"
)

func TestParseIPsAcceptsIPv4AndIPv6EntriesAndRanges(t *testing.T) {
	ips, ranges := parseIPs([]byte(`
deny 192.0.2.8
deny 2001:0db8::8
punctuation 203.0.113.9:
network 198.51.100.0/24
network 2001:db8:abcd::/48
invalid 999.1.1.1
`))

	ipSet := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		ipSet[ip] = struct{}{}
	}
	for _, expected := range []string{"192.0.2.8", "2001:db8::8", "203.0.113.9"} {
		if _, found := ipSet[expected]; !found {
			t.Errorf("parsed IPs do not contain %q: %#v", expected, ips)
		}
	}

	rangeSet := make(map[string]struct{}, len(ranges))
	for _, entry := range ranges {
		rangeSet[entry.CIDR] = struct{}{}
	}
	for _, expected := range []string{"198.51.100.0/24", "2001:db8:abcd::/48"} {
		if _, found := rangeSet[expected]; !found {
			t.Errorf("parsed ranges do not contain %q: %#v", expected, ranges)
		}
	}
}

func TestBlacklistRangeCacheMatchesBothAddressFamilies(t *testing.T) {
	compiled, invalidCount := buildBlacklistRangeCache([]domain.BlacklistedRange{
		{CIDR: "192.0.2.0/24"},
		{CIDR: "192.0.2.128/25"},
		{CIDR: "2001:db8::/32"},
		{CIDR: "not-a-network"},
	})
	if invalidCount != 1 {
		t.Fatalf("invalid range count = %d, want 1", invalidCount)
	}
	if len(compiled.spans) != 2 {
		t.Fatalf("merged span count = %d, want 2", len(compiled.spans))
	}

	testCases := map[string]bool{
		"192.0.2.1":        true,
		"192.0.3.1":        false,
		"2001:db8::1":      true,
		"2001:db9::1":      false,
		"::ffff:192.0.2.1": true,
	}
	for address, expected := range testCases {
		if got := inRange(address, compiled.spans); got != expected {
			t.Errorf("inRange(%q) = %v, want %v", address, got, expected)
		}
	}
}

func TestFilterProxiesBlocksExactAndRangedIPv6Addresses(t *testing.T) {
	previousIPs := cache.Load()
	previousRanges := rangeCache.Load()
	t.Cleanup(func() {
		cache.Store(previousIPs)
		rangeCache.Store(previousRanges)
	})

	cache.Store(map[string]struct{}{"2001:db8::10": {}})
	compiled, _ := buildBlacklistRangeCache([]domain.BlacklistedRange{{CIDR: "2001:db8:abcd::/48"}})
	rangeCache.Store(compiled)

	makeProxy := func(address string) domain.Proxy {
		proxy := domain.Proxy{Port: 8080}
		if err := proxy.SetIP(address); err != nil {
			t.Fatalf("SetIP(%q): %v", address, err)
		}
		return proxy
	}

	allowed, blocked := FilterProxies([]domain.Proxy{
		makeProxy("2001:db8::10"),
		makeProxy("2001:db8:abcd::20"),
		makeProxy("2001:db9::30"),
		func() domain.Proxy {
			proxy := domain.Proxy{Port: 8080}
			if err := proxy.SetHost("gateway.provider.example"); err != nil {
				t.Fatalf("SetHost: %v", err)
			}
			return proxy
		}(),
	})
	if len(blocked) != 2 || len(allowed) != 2 {
		t.Fatalf("blocked/allowed counts = %d/%d, want 2/2", len(blocked), len(allowed))
	}
	if got := allowed[0].GetIp(); got != "2001:db9::30" {
		t.Fatalf("allowed proxy = %q, want 2001:db9::30", got)
	}
	if got := allowed[1].GetHost(); got != "gateway.provider.example" {
		t.Fatalf("allowed hostname route = %q", got)
	}
}
