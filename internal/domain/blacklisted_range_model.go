package domain

// BlacklistedRange stores an IPv4 or IPv6 network used for blacklist enforcement.
type BlacklistedRange struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	// CIDR holds the normalized network string.
	CIDR   string `gorm:"column:cidr;type:cidr;uniqueIndex"`
	Source string `gorm:"size:512;not null;default:''"`
}
