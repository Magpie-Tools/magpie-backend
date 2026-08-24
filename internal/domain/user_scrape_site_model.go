package domain

import "time"

type WorkspaceScrapeSite struct {
	WorkspaceID  uint      `gorm:"column:workspace_id;primaryKey"`
	ScrapeSiteID uint64    `gorm:"primaryKey"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}

func (WorkspaceScrapeSite) TableName() string {
	return "user_scrape_site"
}

type UserScrapeSite = WorkspaceScrapeSite
