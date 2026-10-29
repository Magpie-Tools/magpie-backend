package database

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/support"
)

func TestProxyTypeFromASN(t *testing.T) {
	tests := []struct {
		name string
		asn  uint
		org  string
		want string
	}{
		{"hosting ASN survives branding changes", 63949, "Akamai Connected Cloud", "Datacenter"},
		{"Vultr ASN without a hosting keyword", 20473, "The Constant Company, LLC", "Datacenter"},
		{"hosting ASN wins over ambiguous wording", 14061, "Customer Residential Communications", "Datacenter"},
		{"hosting name fallback", 0, "Hetzner Online GmbH", "Datacenter"},
		{"hosting name wins over generic ISP wording", 0, "Microsoft Communications", "Datacenter"},
		{"OVHcloud name fallback", 0, "OVHcloud", "Datacenter"},
		{"organization case", 0, "DIGITALOCEAN, LLC", "Datacenter"},
		{"Google consumer network", 0, "Google Fiber Inc.", "ISP"},
		{"Google hosting name", 0, "Google LLC", "Datacenter"},
		{"telecommunications", 0, "Example Telecommunications Ltd.", "ISP"},
		{"broadband", 0, "Example Broadband", "ISP"},
		{"AWS substring in a consumer provider", 0, "Crawshaw Communications", "ISP"},
		{"customer wording is inconclusive", 0, "Customer Services Ltd.", "N/A"},
		{"residential wording is inconclusive", 0, "Residential Networks Ltd.", "N/A"},
		{"network wording is inconclusive", 0, "Example Networks", "N/A"},
		{"unrelated hosting substring", 0, "Amazonia Research", "N/A"},
		{"unknown ASN", 64512, "Example Ltd.", "N/A"},
		{"no ASN record", 0, "", "N/A"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := proxyTypeFromASN(tt.asn, tt.org); got != tt.want {
				t.Fatalf("proxy type = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProxyTypeDNSEvidence(t *testing.T) {
	tests := []struct {
		name  string
		names []string
		want  string
	}{
		{"generic IP", []string{"ip-192-0-2-1.example.net."}, "N/A"},
		{"static allocation", []string{"static-192-0-2-1.example.net."}, "N/A"},
		{"dynamic allocation", []string{"dynamic-192-0-2-1.example.net."}, "N/A"},
		{"address pool", []string{"pool-192-0-2-1.example.net."}, "N/A"},
		{"customer allocation", []string{"customer-192-0-2-1.example.net."}, "N/A"},
		{"user allocation", []string{"user-192-0-2-1.example.net."}, "N/A"},
		{"res substring", []string{"presidential-192-0-2-1.example.net."}, "N/A"},
		{"DSL substring", []string{"dslr-camera.example.net."}, "N/A"},
		{"DSL substring with digits", []string{"dsl123camera.example.net."}, "N/A"},
		{"access marker in provider domain", []string{"ip-192-0-2-1.residential.example."}, "N/A"},
		{"access marker in co.uk provider", []string{"ip-192-0-2-1.dsl.co.uk."}, "N/A"},
		{"access marker in top level domain", []string{"ip-192-0-2-1.example.dsl."}, "N/A"},
		{"bare domain", []string{"residential.example."}, "N/A"},
		{"empty hostname", []string{""}, "N/A"},
		{"no PTR record", nil, "N/A"},
		{"ADSL address", []string{"adsl-192-0-2-1.example.net."}, "Residential"},
		{"numbered DSL label", []string{"dsl123.example.net."}, "Residential"},
		{"nested access label", []string{"192-0-2-1.vdsl.example.net."}, "Residential"},
		{"FTTH address", []string{"ftth-192-0-2-1.example.net."}, "Residential"},
		{"cable address", []string{"cable-192-0-2-1.example.net."}, "Residential"},
		{"case and root dot", []string{" ADSL-192-0-2-1.EXAMPLE.NET. "}, "Residential"},
		{"mobile address", []string{"mobile-192-0-2-1.example.net."}, "N/A"},
		{"mobile and access markers", []string{"dsl-192-0-2-1.mobile.example.net."}, "N/A"},
		{"server and access markers", []string{"vps-192-0-2-1.dsl.example.net."}, "N/A"},
		{"conflicting PTR records", []string{"adsl-192-0-2-1.example.net.", "cloud-192-0-2-1.example.org."}, "N/A"},
		{"conflicting PTR records reversed", []string{"cloud-192-0-2-1.example.org.", "adsl-192-0-2-1.example.net."}, "N/A"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := refineProxyTypeWithDNS("N/A", tt.names); got != tt.want {
				t.Fatalf("proxy type = %q, want %q", got, tt.want)
			}
			if got := refineProxyTypeWithDNS("Datacenter", tt.names); got != "Datacenter" {
				t.Fatalf("DNS overwrote hosting classification with %q", got)
			}
			wantISP := tt.want
			if wantISP == "N/A" {
				wantISP = "ISP"
			}
			if got := refineProxyTypeWithDNS("ISP", tt.names); got != wantISP {
				t.Fatalf("ISP proxy type = %q, want %q", got, wantISP)
			}
		})
	}
}

func setupProxyTypeDNSTest(t *testing.T) *atomic.Int32 {
	t.Helper()
	originalCache := dnsCache
	dnsCache = support.NewExpiryCache[string, []string](20_000)
	originalResolver := net.DefaultResolver
	var requests atomic.Int32
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			requests.Add(1)
			return nil, errors.New("unexpected uncached DNS lookup")
		},
	}
	t.Cleanup(func() {
		dnsCache = originalCache
		net.DefaultResolver = originalResolver
	})
	return &requests
}

func TestProxyTypeHostingCandidatesSkipDNS(t *testing.T) {
	requests := setupProxyTypeDNSTest(t)
	var proxies []domain.Proxy
	for _, ip := range []string{"8.8.8.8", "2001:4860:4860::8888"} {
		proxy := domain.Proxy{EstimatedType: "Datacenter"}
		if err := proxy.SetIP(ip); err != nil {
			t.Fatal(err)
		}
		proxies = append(proxies, proxy)
	}
	// Even a stale candidate must not start DNS or overwrite a hosting result.
	if err := applyResidentialOverrides(&proxies, []residentialOverrideCandidate{
		{index: 0, ip: "8.8.8.8"},
		{index: 1, ip: "2001:4860:4860::8888"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, proxy := range proxies {
		if proxy.EstimatedType != "Datacenter" {
			t.Fatalf("hosting classification overwritten with %q", proxy.EstimatedType)
		}
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("hosting classification made %d DNS requests", got)
	}
}

func TestProxyTypeUsesCachedDNSEvidence(t *testing.T) {
	requests := setupProxyTypeDNSTest(t)
	for _, tt := range []struct {
		ip       string
		baseType string
		names    []string
		want     string
	}{
		{"192.0.2.1", "N/A", []string{"static-ip-192-0-2-1.example.net."}, "N/A"},
		{"192.0.2.2", "N/A", []string{"adsl-192-0-2-2.example.net."}, "Residential"},
		{"192.0.2.3", "N/A", nil, "N/A"},
		{"198.51.100.1", "ISP", []string{"static-ip-198-51-100-1.example.net."}, "ISP"},
		{"198.51.100.2", "ISP", []string{"adsl-198-51-100-2.example.net."}, "Residential"},
		{"198.51.100.3", "ISP", []string{"mobile-198-51-100-3.example.net."}, "ISP"},
		{"8.8.8.8", "Datacenter", []string{"adsl-8-8-8-8.example.net."}, "Datacenter"},
	} {
		dnsCache.Set(tt.ip, tt.names, time.Now().Add(dnsCacheTTL))
		if got := refineProxyTypeWithDNS(tt.baseType, getCachedDNS(tt.ip)); got != tt.want {
			t.Errorf("%s classified as %q, want %q", tt.ip, got, tt.want)
		}
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("cached classification made %d DNS requests", got)
	}
}
