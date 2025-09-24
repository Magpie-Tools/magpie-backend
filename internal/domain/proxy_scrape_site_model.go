package domain

import "time"

type ProxyScrapeSite struct {
	ProxyID      uint64    `gorm:"primaryKey"`
	ScrapeSiteID uint64    `gorm:"primaryKey"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}

func (ProxyScrapeSite) TableName() string {
	return "proxy_scrape_site"
}
