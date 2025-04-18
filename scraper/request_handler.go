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
	/* ---------------------- acquire a page ---------------------- */
	var page *rod.Page
	select {
	case page = <-pagePool:
	case <-time.After(5 * time.Second):
		return "", fmt.Errorf("timeout waiting for available page")
	}

	/* ---------------------- recycle on exit --------------------- */
	defer recyclePage(page)

	/* ---------------------- navigate & read --------------------- */
	if err := page.Timeout(timeout).Navigate(url); err != nil {
		return "", fmt.Errorf("navigation failed: %w", err)
	}
	if err := page.WaitLoad(); err != nil {
		return "", fmt.Errorf("wait load failed: %w", err)
	}

	html, err := page.HTML()
	if err != nil {
		return "", fmt.Errorf("failed to get HTML: %w", err)
	}
	return html, nil
}

func resetPage(page *rod.Page) error {
	err := proto.NetworkClearBrowserCookies{}.Call(page)
	if err != nil {
		return fmt.Errorf("clear cookies: %w", err)
	}

	if _, err := page.Eval(`() => {
		localStorage.clear();
		sessionStorage.clear();
		return true;
	}`); err != nil {
		return fmt.Errorf("clear storage: %w", err)
	}
	if err := page.Navigate("about:blank"); err != nil {
		return fmt.Errorf("navigate blank: %w", err)
	}
	if err := page.WaitLoad(); err != nil {
		return fmt.Errorf("wait blank: %w", err)
	}
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
