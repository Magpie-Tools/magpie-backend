package domain

import (
	"time"
)

type WorkspaceJudge struct {
	WorkspaceID uint      `gorm:"column:workspace_id;primaryKey"`
	JudgeID     uint      `gorm:"primaryKey"`
	Regex       string    `gorm:"size:255;not null"` // The regex for the relationship
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (WorkspaceJudge) TableName() string {
	return "user_judges"
}

type UserJudge = WorkspaceJudge

type JudgeWithRegex struct {
	Judge *Judge
	Regex string
}
