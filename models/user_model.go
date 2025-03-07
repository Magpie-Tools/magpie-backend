package models

import (
	"magpie/models/routeModels"
	"time"
)

type User struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	Email    string `gorm:"uniqueIndex;not null;size:255"`
	Password string `gorm:"not null;size:100;check:length(password) >= 8" json:"-"`
	Role     string `gorm:"not null;default:'user';check:role IN ('user', 'admin')"`

	//Settings
	HTTPProtocol     bool   `gorm:"not null;default:false"`
	HTTPSProtocol    bool   `gorm:"not null;default:true"`
	SOCKS4Protocol   bool   `gorm:"not null;default:false"`
	SOCKS5Protocol   bool   `gorm:"not null;default:false"`
	Timeout          uint16 `gorm:"not null;default:7500"`
	Retries          uint8  `gorm:"not null;default:2"`
	UseHttpsForSocks bool   `gorm:"not null;default:true"`

	//Relations
	Judges      []Judge      `gorm:"many2many:user_judges;"`
	Proxies     []Proxy      `gorm:"many2many:user_proxies;"`
	ScrapeSites []ScrapeSite `gorm:"many2many:user_scrape_site;"`
	CreatedAt   time.Time    `gorm:"autoCreateTime"`
}

func (u *User) ToUserSettings() routeModels.UserSettings {
	return routeModels.UserSettings{
		HTTPProtocol:   u.HTTPProtocol,
		HTTPSProtocol:  u.HTTPSProtocol,
		SOCKS4Protocol: u.SOCKS4Protocol,
		SOCKS5Protocol: u.SOCKS5Protocol,
		Timeout:        u.Timeout,
		Retries:        u.Retries,
	}
}
