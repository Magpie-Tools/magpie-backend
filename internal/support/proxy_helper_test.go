package support

import (
	"strings"
	"testing"

	"magpie/internal/domain"
)

func TestClearProxyString(t *testing.T) {
	input := "1.1.1.1@80\r\n5.5.5.5.:443"
	got := clearProxyString(input)

	if strings.ContainsAny(got, "@\r") {
		t.Fatalf("clearProxyString did not strip control characters, got %q", got)
	}
	if strings.Contains(got, "..") {
		t.Fatalf("clearProxyString did not normalize dot sequences, got %q", got)
	}
	if strings.Contains(got, ".:") {
		t.Fatalf("clearProxyString did not normalize dot-colon sequence, got %q", got)
	}
}

func TestParseTextToProxies(t *testing.T) {
	input := "1.1.1.1:80\r\ninvalid\n2.2.2.2:8080:user:pass\n2.2.2.2:badport\n"

	parsed := ParseTextToProxies(input)
	if len(parsed) != 2 {
		t.Fatalf("ParseTextToProxies returned %d proxies, want 2", len(parsed))
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
}

func TestFindIP(t *testing.T) {
	input := "Client address: 203.0.113.5 connected via [2001:db8::1]"

	if got := FindIP(input); got != "203.0.113.5" {
		t.Fatalf("FindIP returned %s, want 203.0.113.5", got)
	}
}

func TestFormatProxies(t *testing.T) {
	proxy := domain.Proxy{Port: 3128, Username: "user", Password: "pass", Country: "US", EstimatedType: "Residential"}
	if err := proxy.SetIP("10.0.0.5"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}
	proxy.Statistics = []domain.ProxyStatistic{{
		Alive:        true,
		ResponseTime: 150,
		Protocol:     domain.Protocol{Name: "https"},
	}}

	format := "protocol ip:port username password country alive type time"
	got := FormatProxies([]domain.Proxy{proxy}, format)
	expected := "https 10.0.0.5:3128 user pass US true Residential 150\n"

	if got != expected {
		t.Fatalf("FormatProxies returned %q, want %q", got, expected)
	}
}
