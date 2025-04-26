package models

import "time"

type ProxyStatistic struct {
	ID            uint64 `gorm:"primaryKey;autoIncrement"`
	Alive         bool   `gorm:"not null"`
	Attempt       uint8  `gorm:"not null"`
	ResponseTime  uint16 `gorm:"not null"`         // Milliseconds
	Country       string `gorm:"size:3;not null"`  // ISO 3166-1 alpha-2
	EstimatedType string `gorm:"size:20;not null"` // ISP, Datacenter, Residential

	// Relationships
	ProtocolID int      `gorm:"index"`
	Protocol   Protocol `gorm:"foreignKey:ProtocolID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	LevelID *int           `gorm:"index"`
	Level   AnonymityLevel `gorm:"foreignKey:LevelID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	ProxyID uint64 `gorm:"not null;index"`
	Proxy   Proxy  `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	JudgeID uint  `gorm:"not null;index"`
	Judge   Judge `gorm:"foreignKey:JudgeID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
}
type AnonymityLevel struct {
	ID   int    `gorm:"primaryKey;autoIncrement"`
	Name string `gorm:"size:50;not null;unique"` // elite, anonymous, transparent
}

type Protocol struct {
	ID   int    `gorm:"primaryKey;autoIncrement"`
	Name string `gorm:"size:6;not null;unique"` //http, https, socks4, socks5
}
