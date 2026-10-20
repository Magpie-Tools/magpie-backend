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

func TestOnlyLocalBrowserFailuresAreDeferred(t *testing.T) {
	for _, err := range []error{errBrowserBusy, fmt.Errorf("launch: %w", errBrowserUnavailable)} {
		if !isScrapeDeferred(err) {
			t.Fatalf("local capacity failure was not deferred: %v", err)
		}
	}
	for _, err := range []error{nil, context.DeadlineExceeded, &scrapeHTTPStatusError{code: 429}, &scrapeHTTPStatusError{code: 503}, &scrapeHTTPStatusError{code: 404}, support.ErrUnsafeOutboundTarget, support.ErrResponseBodyTooLarge} {
		if isScrapeDeferred(err) {
			t.Fatalf("completed fetch was scheduled for an early retry: %v", err)
		}
	}
}

func TestScrapeRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		header string
		want   time.Time
	}{
		{"7200", now.Add(2 * time.Hour)},
		{" 120 ", now.Add(2 * time.Minute)},
		{"0", now},
		{now.Add(30 * time.Hour).Format(http.TimeFormat), now.Add(30 * time.Hour)},
		{"", time.Time{}}, {"-1", time.Time{}}, {"1.5", time.Time{}},
		{"bad", time.Time{}}, {"18446744073709551615", time.Time{}},
		{now.Add(-time.Hour).Format(http.TimeFormat), time.Time{}},
	} {
		if got := parseScrapeRetryAfter(tc.header, now); !got.Equal(tc.want) {
			t.Errorf("Retry-After %q = %v, want %v", tc.header, got, tc.want)
		}
	}
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "true")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "108000")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	before := time.Now()
	_, err := ScraperRequest(server.URL, time.Second)
	var status *scrapeHTTPStatusError
	if !errors.As(err, &status) || status.retryAfter.Before(before.Add(30*time.Hour)) ||
		!strings.Contains(err.Error(), "provider requested no retry before") {
		t.Fatalf("Retry-After deadline was lost: %v", err)
	}
}
