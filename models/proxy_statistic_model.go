package models

type ProxyStatistic struct {
	ID            uint64 `gorm:"primaryKey;autoIncrement"`
	Alive         bool   `gorm:"not null"`
	ResponseTime  int16  `gorm:"not null"`         // Milliseconds
	Country       string `gorm:"size:2;not null"`  // ISO 3166-1 alpha-2
	EstimatedType string `gorm:"size:20;not null"` // ISP, Datacenter, Residential

	// Relationships
	LevelID *int           `gorm:"index"`
	Level   AnonymityLevel `gorm:"foreignKey:LevelID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	ProxyID uint64 `gorm:"not null;index"`
	Proxy   Proxy  `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}
type AnonymityLevel struct {
	ID   int    `gorm:"primaryKey;autoIncrement"`
	Name string `gorm:"size:50;not null;unique"` // elite, anonymous, transparent
}
