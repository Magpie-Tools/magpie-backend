package scraper

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/proto"
)

/*
ScraperRequest fetches the HTML of url within the given timeout.

It borrows a *rod.Page from the global pagePool, does the navigation
and then defers the page‑recycling to recyclePage(), which decides
whether to return the page to the pool or close it (depending on
signals from managePagePool). This keeps the request code tiny while
all pool housekeeping lives in thread_handler.go.
*/
func ScraperRequest(url string, timeout time.Duration) (string, error) {
	// 1) acquire a page with timeout
	var p *rod.Page
	select {
	case p = <-pagePool:
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout waiting for available page")
	}

	// 2) ensure we recycle it back (or close+re-add on error)
	defer recyclePage(p)

	// 3) apply per-request timeout
	p = p.Timeout(timeout)

	// Deny disk downloads for this page; deprecated API but still honored.
	_ = proto.PageSetDownloadBehavior{
		Behavior: proto.PageSetDownloadBehaviorBehaviorDeny,
	}.Call(p)

	// Ensure network events are available so we can pull raw responses.
	_ = proto.NetworkEnable{}.Call(p)

	var (
		capturedBody         string
		capturedMime         string
		capturedDisposition  string
		captured             bool
		done                 = make(chan struct{})
		doneOnce             sync.Once
		responseCaptureError error
	)

	eventCtx, cancelEvents := context.WithCancel(context.Background())
	defer cancelEvents()

	var mainRequestID proto.NetworkRequestID

	waitResponse := p.Context(eventCtx).EachEvent(
		func(e *proto.NetworkRequestWillBeSent) {
			if e.FrameID != "" && e.FrameID != p.FrameID {
				return
			}
			if e.Type == proto.NetworkResourceTypeDocument {
				mainRequestID = e.RequestID
				return
			}
			if mainRequestID == "" && (e.Request.URL == url || e.DocumentURL == url) {
				mainRequestID = e.RequestID
				return
			}
			if mainRequestID == "" && (e.Type == proto.NetworkResourceTypeOther || e.Type == proto.NetworkResourceTypeXHR || e.Type == proto.NetworkResourceTypeFetch) {
				mainRequestID = e.RequestID
			}
		},
		func(e *proto.NetworkResponseReceived) bool {
			if e.FrameID != "" && e.FrameID != p.FrameID {
				return false
			}
			if mainRequestID != "" && e.RequestID != mainRequestID {
				return false
			}

			body, err := proto.NetworkGetResponseBody{RequestID: e.RequestID}.Call(p)
			if err != nil {
				responseCaptureError = err
				doneOnce.Do(func() { close(done) })
				return true
			}

			if body.Base64Encoded {
				raw, decodeErr := base64.StdEncoding.DecodeString(body.Body)
				if decodeErr != nil {
					responseCaptureError = decodeErr
					doneOnce.Do(func() { close(done) })
					return true
				}
				capturedBody = string(raw)
			} else {
				capturedBody = body.Body
			}

			captured = true
			capturedMime = e.Response.MIMEType
			capturedDisposition = headerValue(e.Response.Headers, "Content-Disposition")
			mainRequestID = e.RequestID

			doneOnce.Do(func() { close(done) })
			return true
		},
		func(e *proto.NetworkLoadingFinished) bool {
			if mainRequestID == "" || e.RequestID != mainRequestID {
				return false
			}
			doneOnce.Do(func() { close(done) })
			return true
		},
	)

	go func() {
		waitResponse()
		doneOnce.Do(func() { close(done) })
	}()

	waitWindow := time.Second
	if timeout > 0 && timeout < waitWindow {
		waitWindow = timeout
	}

	navErr := p.Navigate(url)

	select {
	case <-done:
	case <-time.After(waitWindow):
	}

	if responseCaptureError != nil {
		captured = false
	}

	if navErr != nil {
		if captured {
			return capturedBody, nil
		}
		if isNavigationAbortError(navErr) {
			if fallback, err := fetchDirect(url, timeout); err == nil {
				return fallback, nil
			} else {
				return "", fmt.Errorf("navigation aborted and fallback fetch failed: %w", err)
			}
		}
		return "", navErr
	}

	if err := p.WaitLoad(); err != nil {
		if captured {
			return capturedBody, nil
		}
		if isNavigationAbortError(err) {
			if fallback, fallbackErr := fetchDirect(url, timeout); fallbackErr == nil {
				return fallback, nil
			} else {
				return "", fmt.Errorf("navigation aborted and fallback fetch failed: %w", fallbackErr)
			}
		}
		return "", err
	}

	// 4) grab the HTML
	html, err := p.HTML()
	if err != nil {
		if captured {
			return capturedBody, nil
		}
		return "", err
	}
	if captured && shouldPreferCapturedBody(capturedMime, capturedDisposition, html) {
		return capturedBody, nil
	}
	return html, nil
}

func resetPage(page *rod.Page) error {
	// Clear cookies
	err := proto.NetworkClearBrowserCookies{}.Call(page)
	if err != nil {
		return fmt.Errorf("clear cookies: %w", err)
	}

	// Navigate to about:blank first
	if err := page.Navigate("about:blank"); err != nil {
		return fmt.Errorf("navigate blank: %w", err)
	}
	if err := page.WaitLoad(); err != nil {
		return fmt.Errorf("wait blank: %w", err)
	}

	_, _ = page.Eval(`() => {
        try {
            localStorage.clear();
            sessionStorage.clear();
        } catch (e) {
            // Silently ignore security errors
        }
        return true;
    }`)

	return nil
}

func headerValue(headers proto.NetworkHeaders, key string) string {
	for k, v := range headers {
		if strings.EqualFold(k, key) {
			return fmt.Sprint(v)
		}
	}
	return ""
}

func shouldPreferCapturedBody(mime, disposition, html string) bool {
	if disposition != "" && strings.Contains(strings.ToLower(disposition), "attachment") {
		return true
	}

	if mime != "" && !strings.Contains(strings.ToLower(mime), "html") {
		return true
	}

	if strings.TrimSpace(html) == "" {
		return true
	}

	return false
}

func isNavigationAbortError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if msg == "" {
		return false
	}
	abortSignatures := []string{
		"net::ERR_ABORTED",
		"NS_BINDING_ABORTED",
		"ERR_INTERNET_DISCONNECTED",
	}
	for _, sig := range abortSignatures {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

func fetchDirect(url string, timeout time.Duration) (string, error) {
	limit := 30 * time.Second
	if timeout > 0 {
		limit = timeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "magpie-scraper/1.0")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("fallback fetch status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

/*
Cleanup closes every page still in the pool and finally the browser
instance. Call this before a graceful shutdown of your application.
*/
func Cleanup() {
	for {
		select {
		case p := <-pagePool:
			p.MustClose()
		default:
			browser.MustClose()
			return
		}
	}
}
