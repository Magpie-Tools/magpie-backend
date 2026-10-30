package ipgeolocation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestLookupUsesV3AndAccountsForSecurityCredits(t *testing.T) {
	for _, security := range []bool{false, true} {
		t.Run(map[bool]string{false: "basic", true: "security"}[security], func(t *testing.T) {
			client := Client{HTTPClient: &http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Scheme != "https" || req.URL.Host != "api.ipgeolocation.io" || req.URL.Path != "/v3/ipgeo" {
					t.Fatalf("unexpected endpoint: %s", req.URL.Host+req.URL.Path)
				}
				if req.URL.Query().Get("apiKey") != "test/key" || req.URL.Query().Get("ip") != "8.8.8.8" {
					t.Fatal("missing key or canonical IP")
				}
				if (req.URL.Query().Get("include") == "security") != security {
					t.Fatal("wrong security mode")
				}
				fields := "location,network,asn,company"
				if security {
					fields += ",security"
				}
				if req.URL.Query().Get("fields") != fields {
					t.Fatal("unbounded module request")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"X-Credits-Charged": []string{"3"}}, Body: io.NopCloser(strings.NewReader(`{"ip":"8.8.8.8","location":{"country_code2":"US","country_name":"United States"},"company":{"type":"ISP"},"network":{"connection_type":"Mobile"},"security":{"is_proxy":false}}`))}, nil
			})}}
			response, err := client.Lookup(context.Background(), "test/key", "::ffff:8.8.8.8", security)
			if err != nil {
				t.Fatal(err)
			}
			if response.Credits != 3 || response.Result.EstimatedType() != "Mobile" {
				t.Fatalf("wrong response: %+v", response)
			}
			if response.Result.Security.ProxyProviders == nil || response.Result.Security.IsProxy == nil || *response.Result.Security.IsProxy {
				t.Fatal("missing and negative signals were conflated")
			}
		})
	}
}

func TestLookupRejectsUnsafeOrInconsistentData(t *testing.T) {
	for _, body := range []string{`{"ip":"1.1.1.1"}`, `{"ip":"8.8.8.8","location":{"country_code2":"USA"}}`, `{"ip":"8.8.8.8","security":{"proxy_confidence_score":101}}`, `not json`, strings.Repeat("x", 1024*1024+1)} {
		client := Client{HTTPClient: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		response, err := client.Lookup(context.Background(), "secret-key", "8.8.8.8", false)
		if err == nil || response.Credits != 1 {
			t.Fatal("invalid response accepted, or successful HTTP response was not charged")
		}
	}
	for _, ip := range []string{"192.168.1.1", "127.0.0.1", "100.64.1.2", "192.0.2.1", "2001:db8::1", "fe80::1", "::1", "gateway.example"} {
		client := Client{HTTPClient: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { t.Fatal("sent an ineligible IP"); return nil, nil })}}
		if _, err := client.Lookup(context.Background(), "secret-key", ip, false); err == nil {
			t.Fatalf("accepted %s", ip)
		}
	}
}

func TestLookupRedactsErrorsAndHonorsRetryAfter(t *testing.T) {
	key := "never-log-this-key"
	client := Client{HTTPClient: &http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) { return nil, errors.New(req.URL.String()) })}}
	_, err := client.Lookup(context.Background(), key, "8.8.8.8", true)
	if err == nil || strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "apiKey=") {
		t.Fatalf("unsafe error: %v", err)
	}
	client.HTTPClient.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"120"}}, Body: io.NopCloser(strings.NewReader(key))}, nil
	})
	response, err := client.Lookup(context.Background(), key, "8.8.8.8", true)
	if err == nil || strings.Contains(err.Error(), key) || response.RetryAfter != 2*time.Minute || response.Credits != 0 {
		t.Fatalf("incorrect retry/error: %+v %v", response, err)
	}
	client.HTTPClient.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"172800"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	response, _ = client.Lookup(context.Background(), key, "8.8.8.8", true)
	if response.RetryAfter != 48*time.Hour {
		t.Fatal("truncated a long provider deadline", response.RetryAfter)
	}
}

func TestLookupRejectsInvalidCreditHeaders(t *testing.T) {
	for _, charge := range []string{"-1", "invalid", "9223372036854775807"} {
		client := Client{HTTPClient: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"X-Credits-Charged": []string{charge}}, Body: io.NopCloser(strings.NewReader(`{"ip":"8.8.8.8"}`))}, nil
		})}}
		response, err := client.Lookup(context.Background(), "key", "8.8.8.8", false)
		if err == nil || response.Credits != 1 {
			t.Fatal("invalid credit charge was accepted", charge, response, err)
		}
	}
}

func TestNetworkTypeUsesSpecificProviderEvidence(t *testing.T) {
	for _, tt := range []struct{ payload, want string }{
		{`{"company":{"type":"HOSTING"},"security":{"is_residential_proxy":true}}`, "Datacenter"},
		{`{"company":{"type":"ISP"},"network":{"connection_type":"5G"}}`, "Mobile"},
		{`{"company":{"type":"ISP"},"security":{"is_residential_proxy":true}}`, "Residential"},
		{`{"company":{"type":"ISP"},"network":{"connection_type":"Wireless"}}`, "ISP"},
		{`{"network":{"connection_type":"DSL"}}`, "Residential"},
		{`{"asn":{"type":"HOSTING"}}`, "Datacenter"},
		{`{"security":{"is_proxy":false}}`, ""},
		{`{}`, ""},
	} {
		var result Result
		if err := json.Unmarshal([]byte(tt.payload), &result); err != nil {
			t.Fatal(err)
		}
		if got := result.EstimatedType(); got != tt.want {
			t.Fatalf("%s: %s, want %s", tt.payload, got, tt.want)
		}
	}
}
