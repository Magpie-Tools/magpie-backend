package models

import "time"

type UserJudge struct {
	UserID    uint      `gorm:"primaryKey"`
	JudgeID   uint      `gorm:"primaryKey"`
	Regex     string    `gorm:"size:255;not null"` // The regex for the relationship
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (UserJudge) TableName() string {
	return "user_judges"
}
