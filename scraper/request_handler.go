package scraper

import (
	"fmt"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/proto" // import proto for CDP commands
	"github.com/go-rod/stealth"
)

var (
	browser  *rod.Browser
	pagePool chan *rod.Page
	poolSize = 50 // Adjust pool size based on expected concurrency
)

func init() {
	// Launch the browser with default settings
	browser = rod.New().MustConnect()

	// Initialize the page pool with stealth pages
	pagePool = make(chan *rod.Page, poolSize)
	for i := 0; i < poolSize; i++ {
		page, err := stealth.Page(browser)
		if err != nil {
			panic(fmt.Sprintf("Failed to create stealth page: %v", err))
		}
		pagePool <- page
	}
}

func ScraperRequest(url string) (string, error) {
	// Acquire a page from the pool
	page := <-pagePool
	defer func() {
		if err := resetPage(page); err != nil {
			// Replace faulty page with a new one
			page.MustClose()
			newPage, err := stealth.Page(browser)
			if err != nil {
				return // Log error in production
			}
			pagePool <- newPage
		} else {
			pagePool <- page
		}
	}()

	// Navigate to the target URL with timeout
	err := page.Timeout(30 * time.Second).Navigate(url)
	if err != nil {
		return "", fmt.Errorf("navigation failed: %w", err)
	}

	// Wait until all network activity is idle
	if err = page.WaitLoad(); err != nil {
		return "", fmt.Errorf("wait idle failed: %w", err)
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
