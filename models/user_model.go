package models

import "time"

type User struct {
	ID        uint      `gorm:"primaryKey;autoIncrement"`
	Email     string    `gorm:"uniqueIndex;not null;size:255"`
	Password  string    `gorm:"not null;size:100" json:"-"`
	Role      string    `gorm:"not null;default:'user';check:role IN ('user', 'admin')"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}
