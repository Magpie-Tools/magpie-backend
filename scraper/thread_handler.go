package scraper

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	"github.com/go-rod/rod"
	"github.com/go-rod/stealth"
	"magpie/settings"
)

var (
	browser        *rod.Browser
	pagePool       chan *rod.Page
	currentPages   atomic.Int32
	stopPageSignal = make(chan struct{})
)

func init() {
	// Launch the browser with default settings
	browser = rod.New().MustConnect()

	// Initialize the page pool
	pagePool = make(chan *rod.Page, 2000) // Buffer size large enough for maximum expected usage

	// Start the page pool manager
	go managePagePool()
}

func managePagePool() {
	for {
		cfg := settings.GetConfig()

		targetPages := calculateRequiredPages(cfg)

		// Add pages if needed
		for currentPages.Load() < targetPages {
			if err := addPageToPool(); err != nil {
				log.Error("Failed to add page to pool", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
		}

		// Remove pages if we have too many
		for currentPages.Load() > targetPages {
			select {
			case page := <-pagePool:
				page.MustClose()
				currentPages.Add(-1)
			case stopPageSignal <- struct{}{}:
				// Signal sent to reduce pages
			default:
				// If no pages are available in the pool right now, wait and try again
				time.Sleep(100 * time.Millisecond)
			}
		}

		time.Sleep(15 * time.Second)
	}
}

func calculateRequiredPages(cfg settings.Config) int32 {
	// You can adapt this to your specific needs based on:
	// 1. Number of scraping targets
	// 2. Frequency of scraping
	// 3. Average scraping time

	// Get metrics about your scraping workload
	targetCount := getTargetCount()                                                                // Number of sites to scrape
	scrapingIntervalMs := settings.CalculateMillisecondsOfCheckingPeriod(cfg.Scraper.ScraperTimer) // Time between scrapes
	avgScrapingTimeMs := getAverageScrapingTime(cfg)                                               // Average time to complete a scrape

	// Basic calculation: enough pages to handle all targets within the interval
	requiredPages := (targetCount * avgScrapingTimeMs) / scrapingIntervalMs

	// Ensure we have at least 1 page
	if requiredPages < 1 && targetCount > 0 {
		requiredPages = 1
	}

	// Cap at some reasonable maximum
	maxPages := 2000
	if requiredPages > uint64(maxPages) {
		requiredPages = uint64(maxPages)
	}

	return int32(requiredPages)
}

// These functions would need to be implemented based on your specific needs
func getTargetCount() uint64 {
	// Return the number of sites you need to scrape
	// Could come from a database, config, etc.
	return 100 // Example value
}

func getAverageScrapingTime(cfg settings.Config) uint64 {
	// Return the average time in ms it takes to scrape a site
	// Could be calculated from historical data
	return uint64(cfg.Scraper.Timeout * cfg.Scraper.Retries) // Example: 5 seconds
}

func addPageToPool() error {
	page, err := stealth.Page(browser)
	if err != nil {
		return fmt.Errorf("failed to create stealth page: %w", err)
	}

	select {
	case pagePool <- page:
		currentPages.Add(1)
	default:
		// If the channel is full, close the page
		page.MustClose()
		return fmt.Errorf("page pool is full")
	}

	return nil
}
