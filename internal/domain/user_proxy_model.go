package domain

import "time"

type UserProxy struct {
	UserID    uint      `gorm:"primaryKey"`
	ProxyID   uint64    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (UserProxy) TableName() string {
	return "user_proxies"
}
