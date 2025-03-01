package models

import "time"

type ScrapeSite struct {
	ID  uint64 `gorm:"primaryKey;autoIncrement"`
	URL string `gorm:"unique"`

	Users []User `gorm:"many2many:user_scrape_site;"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
}
