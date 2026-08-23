package database

import (
	"net/netip"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestBlacklistNormalizationAcceptsBothAddressFamilies(t *testing.T) {
	ips := dedupeIPs([]domain.BlacklistedIP{
		{IP: "192.0.2.1", Source: "one"},
		{IP: "2001:0db8::1", Source: "two"},
		{IP: "2001:db8::1", Source: "duplicate"},
		{IP: "invalid", Source: "bad"},
	})
	if len(ips) != 2 {
		t.Fatalf("deduped IP count = %d, want 2", len(ips))
	}

	ranges := dedupeRanges([]domain.BlacklistedRange{
		{CIDR: "192.0.2.4/24", Source: "one"},
		{CIDR: "2001:0db8:1::1/48", Source: "two"},
		{CIDR: "2001:db8:1::/48", Source: "duplicate"},
		{CIDR: "invalid", Source: "bad"},
	})
	if len(ranges) != 2 {
		t.Fatalf("deduped range count = %d, want 2", len(ranges))
	}

	seen := make(map[string]struct{}, len(ranges))
	for _, entry := range ranges {
		seen[entry.CIDR] = struct{}{}
	}
	for _, expected := range []string{"192.0.2.0/24", "2001:db8:1::/48"} {
		if _, found := seen[expected]; !found {
			t.Errorf("deduped ranges do not contain %q: %#v", expected, ranges)
		}
	}
}

func TestFindProxiesInIPRangesMatchesIPv4AndIPv6(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "blacklist-range-query-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	db, err := gorm.Open(sqlite.Open("file:blacklist-range-query?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := db.AutoMigrate(&domain.User{}, &domain.Proxy{}, &domain.UserProxy{}); err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	for _, address := range []string{"192.0.2.10", "198.51.100.10", "2001:db8::10", "2001:db9::10"} {
		proxy := domain.Proxy{Port: 8080, Country: "N/A", EstimatedType: "N/A"}
		if err := proxy.SetIP(address); err != nil {
			t.Fatalf("SetIP(%q): %v", address, err)
		}
		if err := db.Create(&proxy).Error; err != nil {
			t.Fatalf("create proxy %q: %v", address, err)
		}
	}

	proxies, err := findProxiesInIPRanges(db, []netip.Prefix{
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("2001:db8::/32"),
	})
	if err != nil {
		t.Fatalf("find proxies: %v", err)
	}
	if len(proxies) != 2 {
		t.Fatalf("matched proxy count = %d, want 2", len(proxies))
	}

	seen := make(map[string]struct{}, len(proxies))
	for _, proxy := range proxies {
		seen[proxy.GetIp()] = struct{}{}
	}
	for _, expected := range []string{"192.0.2.10", "2001:db8::10"} {
		if _, found := seen[expected]; !found {
			t.Errorf("matched proxies do not contain %q: %#v", expected, proxies)
		}
	}
}
