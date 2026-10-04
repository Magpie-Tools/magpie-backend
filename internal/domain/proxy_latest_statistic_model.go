package domain

import "time"

type ProxyLatestStatistic struct {
	WorkspaceID       uint   `gorm:"primaryKey;not null;default:0;index:idx_latest_proxy_workspace_protocol,priority:2"`
	ConfigKey         string `gorm:"primaryKey;size:64;not null;default:''"`
	TransportProtocol string `gorm:"size:8;not null;default:''"`
	ProxyID           uint64 `gorm:"primaryKey;index:idx_latest_proxy_workspace_protocol,priority:1"`
	ProtocolID        int    `gorm:"primaryKey;index:idx_latest_protocol_alive,priority:1;index:idx_latest_proxy_workspace_protocol,priority:3"`
	Alive             bool   `gorm:"not null;index:idx_latest_protocol_alive,priority:2"`
	StatisticID       uint64 `gorm:"not null"`
	ResponseTime      uint16
	Attempt           uint8
	LevelID           *int
	JudgeID           uint
	CheckedAt         time.Time `gorm:"not null;index"`

	// Relationships
	Proxy    Proxy    `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Protocol Protocol `gorm:"foreignKey:ProtocolID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}
