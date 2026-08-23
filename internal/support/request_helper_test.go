package support

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"magpie/internal/domain"
)

func TestCreateTransportUsesBracketedIPv6ProxyAddress(t *testing.T) {
	proxy := domain.Proxy{Port: 8080}
	if err := proxy.SetIP("2001:db8::50"); err != nil {
		t.Fatalf("SetIP returned error: %v", err)
	}
	judge := &domain.Judge{FullString: "https://example.com/check"}

	roundTripper, closeTransport, err := CreateTransport(proxy, judge, "http", TransportTCP)
	if err != nil {
		t.Fatalf("CreateTransport returned error: %v", err)
	}
	defer closeTransport()

	transport, ok := roundTripper.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", roundTripper)
	}
	request := httptest.NewRequest(http.MethodGet, judge.FullString, nil)
	proxyURL, err := transport.Proxy(request)
	if err != nil {
		t.Fatalf("proxy function returned error: %v", err)
	}
	if got := proxyURL.Host; got != "[2001:db8::50]:8080" {
		t.Fatalf("proxy URL host = %q, want [2001:db8::50]:8080", got)
	}
}

func TestCreateTransportUsesProviderHostname(t *testing.T) {
	proxy := domain.Proxy{Port: 3128}
	if err := proxy.SetHost("Gateway.Provider.Example."); err != nil {
		t.Fatalf("SetHost returned error: %v", err)
	}
	judge := &domain.Judge{FullString: "https://example.com/check"}

	roundTripper, closeTransport, err := CreateTransport(proxy, judge, "http", TransportTCP)
	if err != nil {
		t.Fatalf("CreateTransport returned error: %v", err)
	}
	defer closeTransport()

	transport := roundTripper.(*http.Transport)
	request := httptest.NewRequest(http.MethodGet, judge.FullString, nil)
	proxyURL, err := transport.Proxy(request)
	if err != nil {
		t.Fatalf("proxy function returned error: %v", err)
	}
	if got := proxyURL.Host; got != "gateway.provider.example:3128" {
		t.Fatalf("proxy URL host = %q, want gateway.provider.example:3128", got)
	}
}
