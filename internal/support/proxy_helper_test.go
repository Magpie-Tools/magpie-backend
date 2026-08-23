package support

import (
	"strings"
	"testing"

	"magpie/internal/domain"
)

func TestClearProxyString(t *testing.T) {
	input := "user:pass@1.1.1.1:80\r\ngateway..provider.example:443"
	got := clearProxyString(input)

	if strings.Contains(got, "\r") {
		t.Fatalf("clearProxyString did not strip carriage returns, got %q", got)
	}
	if !strings.Contains(got, "@") {
		t.Fatalf("clearProxyString removed the authentication delimiter, got %q", got)
	}
	if !strings.Contains(got, "gateway..provider.example") {
		t.Fatalf("clearProxyString rewrote hostname content, got %q", got)
	}
}

func TestParseTextToProxies(t *testing.T) {
	input := strings.Join([]string{
		"1.1.1.1:80",
		"invalid",
		"2.2.2.2:8080:user:pass",
		"user2:pass2@3.3.3.3:9000",
		"4.4.4.4:1080@user3:pass3",
		"[2001:0db8::10]:3128",
		"v6user:v6pass@[2001:db8::20]:8080",
		"[2001:db8::30]:8081:v6suffix:secret",
		"Gateway.Provider.Example.:8000",
		"hostuser:hostpass@gateway2.provider.example:8001",
		"gateway3.provider.example:8002@hostuser2:hostpass2",
		"gateway4.provider.example:8003:hostuser3:hostpass3",
		"2.2.2.2:badport",
	}, "\r\n")

	parsed := ParseTextToProxies(input)
	if len(parsed) != 11 {
		t.Fatalf("ParseTextToProxies returned %d proxies, want 11: %#v", len(parsed), parsed)
	}

	if got := parsed[0].GetFullProxy(); got != "1.1.1.1:80" {
		t.Fatalf("first proxy was %s, want 1.1.1.1:80", got)
	}

	if parsed[0].HasAuth() {
		t.Fatal("expected first proxy to have no auth")
	}

	if got := parsed[1].GetFullProxy(); got != "2.2.2.2:8080" {
		t.Fatalf("second proxy was %s, want 2.2.2.2:8080", got)
	}

	if !parsed[1].HasAuth() {
		t.Fatal("expected second proxy to have auth credentials")
	}
	if parsed[1].Username != "user" || parsed[1].Password != "pass" {
		t.Fatalf("unexpected credentials: %s:%s", parsed[1].Username, parsed[1].Password)
	}

	if parsed[2].Username != "user2" || parsed[2].Password != "pass2" {
		t.Fatalf("unexpected prefix credentials: %s:%s", parsed[2].Username, parsed[2].Password)
	}
	if parsed[3].Username != "user3" || parsed[3].Password != "pass3" {
		t.Fatalf("unexpected suffix credentials: %s:%s", parsed[3].Username, parsed[3].Password)
	}
	if got := parsed[4].GetFullProxy(); got != "[2001:db8::10]:3128" {
		t.Fatalf("IPv6 proxy was %s, want [2001:db8::10]:3128", got)
	}
	if parsed[5].Username != "v6user" || parsed[5].Password != "v6pass" {
		t.Fatalf("unexpected IPv6 prefix credentials: %s:%s", parsed[5].Username, parsed[5].Password)
	}
	if parsed[6].Username != "v6suffix" || parsed[6].Password != "secret" {
		t.Fatalf("unexpected IPv6 colon credentials: %s:%s", parsed[6].Username, parsed[6].Password)
	}
	if got := parsed[7].GetFullProxy(); got != "gateway.provider.example:8000" {
		t.Fatalf("provider hostname proxy was %s", got)
	}
	if parsed[8].Username != "hostuser" || parsed[8].Password != "hostpass" {
		t.Fatalf("unexpected hostname prefix credentials: %s:%s", parsed[8].Username, parsed[8].Password)
	}
	if parsed[9].Username != "hostuser2" || parsed[9].Password != "hostpass2" {
		t.Fatalf("unexpected hostname suffix credentials: %s:%s", parsed[9].Username, parsed[9].Password)
	}
	if parsed[10].Username != "hostuser3" || parsed[10].Password != "hostpass3" {
		t.Fatalf("unexpected hostname colon credentials: %s:%s", parsed[10].Username, parsed[10].Password)
	}
}

func TestParseScrapedTextToIPv4ProxiesKeepsIPv4OnlyBoundary(t *testing.T) {
	input := "3.3.3.3:8080:user:pass\nuser:pass@4.4.4.4:9000\n[2001:db8::1]:8080\ngateway.provider.example:8080\n"

	parsed := ParseScrapedTextToIPv4Proxies(input)
	if len(parsed) != 2 {
		t.Fatalf("ParseScrapedTextToIPv4Proxies returned %d proxies, want 2", len(parsed))
	}

	if parsed[0].HasAuth() {
		t.Fatal("expected first proxy to have no auth")
	}
	if got := parsed[0].GetFullProxy(); got != "3.3.3.3:8080" {
		t.Fatalf("first proxy was %s, want 3.3.3.3:8080", got)
	}

	if !parsed[1].HasAuth() {
		t.Fatal("expected second proxy to have auth credentials")
	}
	if parsed[1].Username != "user" || parsed[1].Password != "pass" {
		t.Fatalf("unexpected credentials: %s:%s", parsed[1].Username, parsed[1].Password)
	}
}

func TestParseTextToProxiesWithStatsAcceptsIPv6(t *testing.T) {
	parsed, stats := ParseTextToProxiesWithStats(strings.Join([]string{
		"[2001:db8::1]:8080",
		"[2001:db8::2]:bad",
		"[not-an-ip]:8080",
		"2001:db8::3:8080",
		"bad_host.example:8080",
		"Gateway.Provider.Example.:9000",
	}, "\n"))

	if len(parsed) != 2 {
		t.Fatalf("parsed proxy count = %d, want 2", len(parsed))
	}
	if stats.SubmittedCount != 6 || stats.ParsedCount != 2 {
		t.Fatalf("submitted/parsed counts = %d/%d, want 6/2", stats.SubmittedCount, stats.ParsedCount)
	}
	if stats.InvalidIPv4Count != 0 {
		t.Fatalf("invalid IPv4 count = %d, want 0 for manual IPv6 import", stats.InvalidIPv4Count)
	}
	if stats.InvalidPortCount != 1 || stats.InvalidAddressCount != 2 || stats.InvalidIPCount != 2 || stats.InvalidFormatCount != 1 {
		t.Fatalf("invalid counts = port %d, address %d, IP alias %d, format %d", stats.InvalidPortCount, stats.InvalidAddressCount, stats.InvalidIPCount, stats.InvalidFormatCount)
	}
}

func TestFindIP(t *testing.T) {
	input := "Client address: [2001:0db8::1] connected via 203.0.113.5"

	if got := FindIP(input); got != "2001:db8::1" {
		t.Fatalf("FindIP returned %s, want 2001:db8::1", got)
	}
}

func TestFormatProxies(t *testing.T) {
	proxy := domain.Proxy{Port: 3128, Username: "user", Password: "pass", Country: "United States", EstimatedType: "Residential"}
	if err := proxy.SetIP("10.0.0.5"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}
	proxy.Statistics = []domain.ProxyStatistic{{
		Alive:        true,
		ResponseTime: 150,
		Protocol:     domain.Protocol{Name: "https"},
	}}
	proxy.Reputations = []domain.ProxyReputation{
		{
			Kind:  "https",
			Label: "good",
			Score: 88.75,
		},
		{
			Kind:  domain.ProxyReputationKindOverall,
			Label: "good",
			Score: 91.25,
		},
	}

	format := "protocol ip:port username password country alive type time reputation reputation_score"
	got := FormatProxies([]domain.Proxy{proxy}, format)
	expected := "https 10.0.0.5:3128 user pass United States true Residential 150 good 88.75\n"

	if got != expected {
		t.Fatalf("FormatProxies returned %q, want %q", got, expected)
	}
}

func TestFormatProxyBracketsIPv6AddressWithPort(t *testing.T) {
	proxy := domain.Proxy{Port: 8080}
	if err := proxy.SetIP("2001:db8::7"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}

	if got := FormatProxy(proxy, "http://ip:port"); got != "http://[2001:db8::7]:8080" {
		t.Fatalf("FormatProxy returned %q, want bracketed IPv6 URL", got)
	}
}

func TestFormatProxyUsesProviderHostname(t *testing.T) {
	proxy := domain.Proxy{Port: 8080}
	if err := proxy.SetHost("Gateway.Provider.Example."); err != nil {
		t.Fatalf("SetHost returned error: %v", err)
	}

	if got := FormatProxy(proxy, "http://ip:port"); got != "http://gateway.provider.example:8080" {
		t.Fatalf("FormatProxy returned %q", got)
	}
}
