package domain

import "time"

type UserProxy struct {
	UserID              uint      `gorm:"primaryKey"`
	ProxyID             uint64    `gorm:"primaryKey"`
	ConsecutiveFailures uint16    `gorm:"not null;default:0"`
	CreatedAt           time.Time `gorm:"autoCreateTime"`
}

func (UserProxy) TableName() string {
	return "user_proxies"
}
