package domain

import "time"

// One durable, coalesced refresh request per route. Version fences completion
// when newer statistics arrive while a worker is calculating reputation.
type ProxyReputationRefresh struct {
	ProxyID     uint64    `gorm:"primaryKey;autoIncrement:false"`
	Version     uint64    `gorm:"not null;default:1"`
	RequestedAt time.Time `gorm:"not null;index:idx_reputation_refresh_pending,priority:1"`
	LeaseUntil  time.Time `gorm:"not null;index:idx_reputation_refresh_pending,priority:2"`
}
