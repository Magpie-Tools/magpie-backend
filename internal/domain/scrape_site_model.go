package domain

import "time"

type ScrapeSite struct {
	ID  uint64 `gorm:"primaryKey;autoIncrement"`
	URL string `gorm:"unique"`

	Proxies    []Proxy     `gorm:"many2many:proxy_scrape_site;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Workspaces []Workspace `gorm:"many2many:user_scrape_site;joinForeignKey:ScrapeSiteID;joinReferences:WorkspaceID;"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
}
