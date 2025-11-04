package domain

import "time"

const (
	ProxyReputationKindOverall = "overall"
)

type ProxyReputation struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	ProxyID      uint64    `gorm:"not null;uniqueIndex:idx_proxy_reputation_proxy_kind,priority:1"`
	Kind         string    `gorm:"size:16;not null;uniqueIndex:idx_proxy_reputation_proxy_kind,priority:2"`
	Score        float32   `gorm:"type:numeric(5,2);not null"`
	Label        string    `gorm:"size:16;not null"`
	Signals      []byte    `gorm:"type:jsonb"`
	CalculatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`

	Proxy Proxy `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}
