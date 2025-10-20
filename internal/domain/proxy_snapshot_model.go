package domain

import "time"

const (
	ProxySnapshotMetricAlive   = "alive"
	ProxySnapshotMetricScraped = "scraped"
)

type ProxySnapshot struct {
	ID        uint      `gorm:"primaryKey;autoIncrement"`
	UserID    uint      `gorm:"not null;index:idx_proxy_snapshot_user_metric,priority:1"`
	Metric    string    `gorm:"size:32;not null;index:idx_proxy_snapshot_user_metric,priority:2"`
	Count     int64     `gorm:"not null"`
	CreatedAt time.Time `gorm:"autoCreateTime"`

	User User `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;" json:"-"`
}
