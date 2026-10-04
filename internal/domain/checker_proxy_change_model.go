package domain

import "time"

// One coalesced revision per route makes membership changes recoverable after
// a missed notification. Deleted routes retain tombstones until maintenance
// advances the full-refresh watermark and removes them.
type CheckerProxyChange struct {
	WorkspaceID uint      `gorm:"primaryKey;index:idx_checker_proxy_changes_revision,priority:1"`
	ProxyID     uint64    `gorm:"primaryKey;index:idx_checker_proxy_changes_revision,priority:3"`
	Revision    uint64    `gorm:"not null;index:idx_checker_proxy_changes_revision,priority:2"`
	Deleted     bool      `gorm:"not null;default:false"`
	UpdatedAt   time.Time `gorm:"not null;default:CURRENT_TIMESTAMP;index:idx_checker_proxy_changes_deleted_at,where:deleted = true"`
}
