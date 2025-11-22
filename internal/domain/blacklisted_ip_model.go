package domain

import "time"

// BlacklistedIP stores normalized IPs that were fetched from blacklist sources.
type BlacklistedIP struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	// IP holds the IPv4 address string (normalized, e.g. 192.0.2.1).
	IP string `gorm:"size:45;uniqueIndex;not null"`

	// Source records the last blacklist source that reported this IP.
	Source string `gorm:"size:512;not null;default:''"`

	FirstSeenAt time.Time `gorm:"autoCreateTime"`
	LastSeenAt  time.Time `gorm:"autoUpdateTime"`
}
