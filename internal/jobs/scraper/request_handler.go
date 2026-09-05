package scraper

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-rod/rod/lib/proto"
	"github.com/go-rod/stealth"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/support"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	scraperUserAgent               = "magpie-scraper/1.0"
	envScraperFallbackMaxBodyBytes = "SCRAPER_FALLBACK_MAX_RESPONSE_BODY_BYTES"
	envScraperCapturedMaxBodyBytes = "SCRAPER_CAPTURED_MAX_RESPONSE_BODY_BYTES"
	defaultScraperFallbackMaxBody  = 8 << 20
	defaultScraperCapturedMaxBody  = 8 << 20
)

func effectiveScrapeTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 30 * time.Second
	}
	return timeout
}

// ScraperRequest uses HTTP. Browser rendering is an explicit source setting.
func ScraperRequest(url string, timeout time.Duration) (string, error) {
	return ScraperRequestContext(context.Background(), url, domain.ScrapeFetchHTTP, timeout)
}

func ScraperRequestContext(parent context.Context, url, mode string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(parent, effectiveScrapeTimeout(timeout))
	defer cancel()
	if !domain.ValidScrapeFetchMode(mode) {
		return "", fmt.Errorf("invalid scrape fetch mode %q", mode)
	}
	if config.IsWebsiteBlocked(url) {
		return "", fmt.Errorf("scrape blocked by website blacklist: %s", url)
	}
	if mode == domain.ScrapeFetchHTTP {
		return fetchDirectContext(ctx, url)
	}
	if _, err := support.ValidateOutboundHTTPURLContext(ctx, url); err != nil {
		return "", err
	}
	return fetchBrowser(ctx, url, sharedBrowser())
}

func fetchBrowser(ctx context.Context, url string, manager *browserManager) (html string, err error) {
	// Rod event setup can panic on a disconnected DevTools socket.
	defer func() {
		if recovered := recover(); recovered != nil {
			html = ""
			err = fmt.Errorf("%w: %v", errBrowserUnavailable, recovered)
		}
	}()
	instance, release, err := manager.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	session, err := instance.browser.Context(ctx).Incognito()
	if err != nil {
		manager.invalidate(instance)
		return "", fmt.Errorf("%w: create browser context: %v", errBrowserUnavailable, err)
	}
	// Dispose the complete context with a fresh, bounded deadline, even when the
	// navigation deadline expired. No page recycling or shared cookie state.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), browserCleanupTimeout)
		defer cancel()
		if closeErr := session.Context(cleanupCtx).Close(); closeErr != nil {
			manager.invalidate(instance)
		}
		if isConnClosed(err) {
			manager.invalidate(instance)
		}
	}()
	page, err := stealth.Page(session)
	if err != nil {
		return "", err
	}
	if err := (proto.BrowserSetDownloadBehavior{Behavior: proto.BrowserSetDownloadBehaviorBehaviorDeny, BrowserContextID: session.BrowserContextID}).Call(session); err != nil {
		return "", err
	}
	if err := (proto.FetchEnable{Patterns: []*proto.FetchRequestPattern{{URLPattern: "http://*"}, {URLPattern: "https://*"}}}).Call(page); err != nil {
		return "", err
	}

	eventCtx, cancelEvents := context.WithCancel(ctx)
	var guardMu sync.Mutex
	var guardErr error
	reject := func(e error) {
		guardMu.Lock()
		if guardErr == nil {
			guardErr = e
		}
		guardMu.Unlock()
	}
	waitEvents := page.Context(eventCtx).EachEvent(
		func(e *proto.FetchRequestPaused) {
			if e == nil || e.Request == nil {
				return
			}
			requestErr := error(nil)
			if config.IsWebsiteBlocked(e.Request.URL) {
				requestErr = fmt.Errorf("browser request blocked by website blacklist")
			} else {
				validationCtx, cancel := context.WithTimeout(eventCtx, 2*time.Second)
				_, requestErr = support.ValidateOutboundHTTPURLContext(validationCtx, e.Request.URL)
				cancel()
			}
			if requestErr != nil {
				reject(requestErr)
				_ = (proto.FetchFailRequest{RequestID: e.RequestID, ErrorReason: proto.NetworkErrorReasonAccessDenied}).Call(page)
				_ = page.StopLoading()
				return
			}
			if e := (proto.FetchContinueRequest{RequestID: e.RequestID}).Call(page); e != nil {
				reject(e)
			}
		},
		func(e *proto.NetworkResponseReceived) {
			if e.FrameID != page.FrameID || e.Type != proto.NetworkResourceTypeDocument {
				return
			}
			if e.Response.Status >= 400 {
				reject(&scrapeHTTPStatusError{code: e.Response.Status})
			}
			if e := validateScrapeRemoteIP(e.Response.RemoteIPAddress); e != nil {
				reject(e)
			}
		},
	)
	eventsDone := make(chan struct{})
	go func() { defer close(eventsDone); waitEvents() }()
	// Keep interception running through load, scripts and DOM extraction. Stopping
	// on the first response leaves later browser requests paused indefinitely.
	defer func() {
		cancelEvents()
		<-eventsDone
		guardMu.Lock()
		defer guardMu.Unlock()
		if guardErr != nil {
			html = ""
			err = guardErr
		}
	}()
	waitIdle := page.WaitRequestIdle(300*time.Millisecond, nil, nil, nil)
	if err := page.Navigate(url); err != nil {
		return "", err
	}
	if err := page.WaitLoad(); err != nil {
		return "", err
	}
	waitIdle()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	html, err = page.HTML()
	if err == nil && int64(len(html)) > scraperCapturedMaxResponseBodyBytes() {
		return "", fmt.Errorf("rendered HTML exceeds response body limit")
	}
	return html, err
}

var directHTTPClient = func() *http.Client {
	client := support.NewRestrictedOutboundHTTPClient(0)
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("too many redirects")
		}
		if config.IsWebsiteBlocked(req.URL.String()) {
			return fmt.Errorf("redirect blocked by website blacklist")
		}
		_, err := support.ValidateOutboundHTTPURLContext(req.Context(), req.URL.String())
		return err
	}
	return client
}()

type scrapeHTTPStatusError struct{ code int }

func (e *scrapeHTTPStatusError) Error() string { return fmt.Sprintf("source returned HTTP %d", e.code) }
func shouldRetryScrape(err error) bool {
	var status *scrapeHTTPStatusError
	return errors.Is(err, errBrowserBusy) || errors.Is(err, errBrowserUnavailable) ||
		errors.Is(err, context.DeadlineExceeded) || isConnClosed(err) ||
		(errors.As(err, &status) && (status.code == 429 || status.code >= 500))
}

func fetchDirectContext(ctx context.Context, url string) (string, error) {
	if config.IsWebsiteBlocked(url) {
		return "", fmt.Errorf("scrape blocked by website blacklist: %s", url)
	}
	validatedURL, err := support.ValidateOutboundHTTPURLContext(ctx, url)
	if err != nil {
		return "", fmt.Errorf("unsafe HTTP target: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, validatedURL.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", scraperUserAgent)
	resp, err := directHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", &scrapeHTTPStatusError{code: resp.StatusCode}
	}
	body, err := support.ReadAllWithLimit(resp.Body, scraperFallbackMaxResponseBodyBytes())
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func scraperFallbackMaxResponseBodyBytes() int64 {
	limit := support.GetEnvInt(envScraperFallbackMaxBodyBytes, defaultScraperFallbackMaxBody)
	if limit <= 0 {
		limit = defaultScraperFallbackMaxBody
	}
	return int64(limit)
}

func scraperCapturedMaxResponseBodyBytes() int64 {
	limit := support.GetEnvInt(envScraperCapturedMaxBodyBytes, defaultScraperCapturedMaxBody)
	if limit <= 0 {
		limit = defaultScraperCapturedMaxBody
	}
	return int64(limit)
}

func validateScrapeRemoteIP(rawIP string) error {
	if strings.TrimSpace(rawIP) == "" {
		return nil
	}
	return support.ValidateOutboundIPLiteral(rawIP)
}
