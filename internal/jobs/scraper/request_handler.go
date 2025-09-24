package scraper

import (
	"fmt"
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
	if err := p.Navigate(url); err != nil {
		return "", err
	}
	p.WaitLoad()

	// 4) grab the HTML
	html, err := p.HTML()
	if err != nil {
		return "", err
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
