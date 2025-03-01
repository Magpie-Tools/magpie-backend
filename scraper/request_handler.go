package scraper

import (
	"fmt"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/proto" // import proto for CDP commands
)

func ScraperRequest(url string, timeout time.Duration) (string, error) {
	// Try to get a page from the pool with timeout
	var page *rod.Page
	select {
	case p := <-pagePool:
		page = p
	case <-time.After(5 * time.Second):
		return "", fmt.Errorf("timeout waiting for available page")
	}

	defer func() {
		select {
		case <-stopPageSignal:
			// This page should be closed to reduce pool size
			page.MustClose()
			currentPages.Add(-1)
		default:
			// Return the page to the pool after cleaning
			if err := resetPage(page); err != nil {
				// Replace faulty page
				page.MustClose()
				addPageToPool() // Add a new page asynchronously
			} else {
				pagePool <- page
			}
		}
	}()

	// Navigate to the target URL with timeout
	err := page.Timeout(timeout).Navigate(url)
	if err != nil {
		return "", fmt.Errorf("navigation failed: %w", err)
	}

	// Wait until page is loaded
	if err = page.WaitLoad(); err != nil {
		return "", fmt.Errorf("wait load failed: %w", err)
	}

	// Retrieve the HTML content
	html, err := page.HTML()
	if err != nil {
		return "", fmt.Errorf("failed to get HTML: %w", err)
	}

	return html, nil
}

// resetPage clears cookies, storage, and navigates to a blank page
func resetPage(page *rod.Page) error {
	// Clear cookies using the DevTools Protocol command
	err := proto.NetworkClearBrowserCookies{}.Call(page)
	if err != nil {
		return fmt.Errorf("clear cookies: %w", err)
	}

	// Clear localStorage and sessionStorage via JavaScript
	_, err = page.Eval(`() => {
		localStorage.clear();
		sessionStorage.clear();
		return true;
	}`)
	if err != nil {
		return fmt.Errorf("clear storage: %w", err)
	}

	// Navigate to about:blank to reset the page state
	if err = page.Navigate("about:blank"); err != nil {
		return fmt.Errorf("navigate to blank: %w", err)
	}

	// Ensure the blank page loads
	if err = page.WaitLoad(); err != nil {
		return fmt.Errorf("wait for blank page: %w", err)
	}

	return nil
}

// Cleanup function to close all pages and the browser
func Cleanup() {
	// Close all pages in the pool
	for {
		select {
		case page := <-pagePool:
			page.MustClose()
		default:
			// No more pages in the pool
			browser.MustClose()
			return
		}
	}
}
