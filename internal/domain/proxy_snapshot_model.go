package domain

import "time"

const (
	ProxySnapshotMetricAlive   = "alive"
	ProxySnapshotMetricScraped = "scraped"
)

type ProxySnapshot struct {
	ID          uint      `gorm:"primaryKey;autoIncrement"`
	WorkspaceID uint      `gorm:"column:workspace_id;not null;index:idx_proxy_snapshot_workspace_metric,priority:1"`
	Metric      string    `gorm:"size:32;not null;index:idx_proxy_snapshot_workspace_metric,priority:2"`
	Count       int64     `gorm:"not null"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`

	Workspace Workspace `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
}
