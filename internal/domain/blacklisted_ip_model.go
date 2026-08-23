package domain

// BlacklistedIP stores normalized IPs that were fetched from blacklist sources.
type BlacklistedIP struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	// IP holds a normalized IPv4 or IPv6 address string.
	IP string `gorm:"type:inet;uniqueIndex;not null"`

	// Source records the last blacklist source that reported this IP.
	Source string `gorm:"size:512;not null;default:''"`
}
