package domain

import "time"

const (
	ScrapeFetchHTTP    = "http"
	ScrapeFetchBrowser = "browser"
)

func ValidScrapeFetchMode(mode string) bool {
	return mode == ScrapeFetchHTTP || mode == ScrapeFetchBrowser
}

type WorkspaceScrapeSite struct {
	// The schema default preserves browser rendering for existing associations.
	// New source imports explicitly select HTTP unless requested otherwise.
	FetchMode            string `gorm:"not null;default:browser"`
	LastScrapedAt        *time.Time
	LastScrapeStatus     string    `gorm:"not null;default:''"`
	LastScrapeError      string    `gorm:"not null;default:''"`
	LastScrapeProxyCount int       `gorm:"not null;default:0"`
	WorkspaceID          uint      `gorm:"column:workspace_id;primaryKey"`
	ScrapeSiteID         uint64    `gorm:"primaryKey"`
	CreatedAt            time.Time `gorm:"autoCreateTime"`
}

func (WorkspaceScrapeSite) TableName() string {
	return "user_scrape_site"
}

type UserScrapeSite = WorkspaceScrapeSite
