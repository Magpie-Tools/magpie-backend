package domain

import "time"

// BlacklistedRange stores IPv4 ranges (CIDRs expanded to start/end) for blacklist enforcement.
type BlacklistedRange struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	StartIP uint32 `gorm:"not null;index"` // inclusive
	EndIP   uint32 `gorm:"not null;index"` // inclusive
	Source  string `gorm:"size:512;not null;default:''"`

	FirstSeenAt time.Time `gorm:"autoCreateTime"`
	LastSeenAt  time.Time `gorm:"autoUpdateTime"`
}
