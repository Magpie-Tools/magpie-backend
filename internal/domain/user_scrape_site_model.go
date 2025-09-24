package domain

import "time"

type UserScrapeSite struct {
	UserID       uint      `gorm:"primaryKey"`
	ScrapeSiteID uint64    `gorm:"primaryKey"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}

func (UserScrapeSite) TableName() string {
	return "user_scrape_site"
}
