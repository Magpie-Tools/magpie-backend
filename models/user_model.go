package models

import "time"

type User struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	Email    string `gorm:"uniqueIndex;not null;size:255"`
	Password string `gorm:"not null;size:100;check:length(password) >= 8" json:"-"`
	Role     string `gorm:"not null;default:'user';check:role IN ('user', 'admin')"`

	Judges      []Judge      `gorm:"many2many:user_judges;"`
	Proxies     []Proxy      `gorm:"many2many:user_proxies;"`
	ScrapeSites []ScrapeSite `gorm:"many2many:user_scrape_site;"`
	CreatedAt   time.Time    `gorm:"autoCreateTime"`
}
