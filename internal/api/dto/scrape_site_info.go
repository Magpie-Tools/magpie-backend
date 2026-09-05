package dto

import "time"

type ScrapeSiteInfo struct {
	FetchMode            string     `json:"fetch_mode"`
	LastScrapedAt        *time.Time `json:"last_scraped_at"`
	LastScrapeStatus     string     `json:"last_scrape_status"`
	LastScrapeError      string     `json:"last_scrape_error"`
	LastScrapeProxyCount int        `json:"last_scrape_proxy_count"`

	Id           uint64    `json:"id"`
	Url          string    `json:"url"`
	ProxyCount   uint      `json:"proxy_count"`
	AliveCount   uint      `json:"alive_count"`
	DeadCount    uint      `json:"dead_count"`
	UnknownCount uint      `json:"unknown_count"`
	AddedAt      time.Time `json:"-"`
}
