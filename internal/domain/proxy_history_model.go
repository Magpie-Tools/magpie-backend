package domain

import "time"

type ProxyHistory struct {
	ID         uint      `gorm:"primaryKey;autoIncrement"`
	UserID     uint      `gorm:"not null;index"`
	ProxyCount int64     `gorm:"not null"`
	CreatedAt  time.Time `gorm:"autoCreateTime"`

	User User `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
}
