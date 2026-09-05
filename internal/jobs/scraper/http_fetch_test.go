package scraper

import (
	"context"
	"errors"
	"fmt"
	"magpie/internal/domain"
	"magpie/internal/support"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPSourceFetchAndLimits(t *testing.T) {
	originalBrowser := sharedBrowser
	sharedBrowser = func() *browserManager { t.Fatal("HTTP fetching tried to initialize Chromium"); return nil }
	t.Cleanup(func() { sharedBrowser = originalBrowser })
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "true")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/error":
			http.Error(w, "failed", 503)
		case "/large":
			fmt.Fprint(w, strings.Repeat("x", 100))
		case "/slow":
			<-r.Context().Done()
		default:
			fmt.Fprint(w, "8.8.8.8:8080")
		}
	}))
	defer server.Close()
	body, err := ScraperRequestContext(context.Background(), server.URL, domain.ScrapeFetchHTTP, time.Second)
	if err != nil || body != "8.8.8.8:8080" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if _, err := ScraperRequest(server.URL+"/error", time.Second); err == nil {
		t.Fatal("HTTP error treated as success")
	}
	t.Setenv(envScraperFallbackMaxBodyBytes, "20")
	if _, err := ScraperRequest(server.URL+"/large", time.Second); !errors.Is(err, support.ErrResponseBodyTooLarge) {
		t.Fatalf("size limit: %v", err)
	}
	if _, err := ScraperRequest(server.URL+"/slow", 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
}

func TestHTTPRejectsPrivateNetwork(t *testing.T) {
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "false")
	if _, err := ScraperRequest("http://127.0.0.1/internal", time.Second); !errors.Is(err, support.ErrUnsafeOutboundTarget) {
		t.Fatalf("unsafe target was allowed: %v", err)
	}
}

func TestScrapeRetryPolicy(t *testing.T) {
	for _, err := range []error{errBrowserBusy, errBrowserUnavailable, context.DeadlineExceeded, &scrapeHTTPStatusError{429}, &scrapeHTTPStatusError{503}} {
		if !shouldRetryScrape(err) {
			t.Fatalf("transient failure was not retried: %v", err)
		}
	}
	for _, err := range []error{nil, &scrapeHTTPStatusError{404}, support.ErrUnsafeOutboundTarget, support.ErrResponseBodyTooLarge} {
		if shouldRetryScrape(err) {
			t.Fatalf("permanent failure was scheduled for rapid retry: %v", err)
		}
	}
}
