package domain

import "time"

type ProxyHistory struct {
	ID          uint      `gorm:"primaryKey;autoIncrement"`
	WorkspaceID uint      `gorm:"column:workspace_id;not null;index"`
	ProxyCount  int64     `gorm:"not null"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`

	Workspace Workspace `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
}
