package domain

import "time"

type ProxyStatistic struct {
	ID                uint64                   `gorm:"primaryKey;autoIncrement;index:idx_proxy_statistics_proxy_created_id,sort:desc,priority:3"`
	Alive             bool                     `gorm:"not null"`
	Attempt           uint8                    `gorm:"not null"`
	ResponseTime      uint16                   `gorm:"not null"` // Milliseconds
	ResponseBody      string                   `gorm:"type:text"`
	TransportProtocol string                   `gorm:"size:8;not null;default:''"`
	CheckTimeout      uint16                   `gorm:"not null;default:0"`
	CheckRetries      uint8                    `gorm:"not null;default:0"`
	CheckEvidence     []WorkspaceCheckEvidence `gorm:"serializer:json;type:jsonb" json:"CheckEvidence,omitempty"`

	// Relationships
	ProtocolID int      `gorm:"index"`
	Protocol   Protocol `gorm:"foreignKey:ProtocolID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	LevelID *int           `gorm:"index"`
	Level   AnonymityLevel `gorm:"foreignKey:LevelID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	ProxyID uint64 `gorm:"not null;index;index:idx_proxy_statistics_proxy_created_id,priority:1"`
	Proxy   Proxy  `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	JudgeID uint  `gorm:"not null;index"`
	Judge   Judge `gorm:"foreignKey:JudgeID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// WorkspaceIDs travels with the asynchronous statistics payload so usage
	// can be attributed without querying ownership in the checker hot path.
	WorkspaceIDs []uint `gorm:"-" json:"WorkspaceIDs,omitempty"`

	// Stream identity is assigned by the consumer, never by the producer.
	EventStream string `gorm:"-" json:"-"`
	EventID     string `gorm:"-" json:"-"`

	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_proxy_statistics_proxy_created_id,sort:desc,priority:2"`
}

// ProxyStatisticEvent protects committed entries above the durable replay floor.
type ProxyStatisticEvent struct {
	Stream    string    `gorm:"primaryKey;size:255;index:idx_statistic_events_stream_sort,priority:1"`
	EventID   string    `gorm:"primaryKey;size:64"`
	SortID    string    `gorm:"size:41;not null;default:'';index:idx_statistic_events_stream_sort,priority:2"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// ProxyStatisticStream rejects historical replays after their individual
// identities have been retired. The floor is exclusive and never moves back.
type ProxyStatisticStream struct {
	Stream       string    `gorm:"primaryKey;size:255"`
	ReplayBefore string    `gorm:"size:41;not null;default:''"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`
}

type WorkspaceCheckEvidence struct {
	WorkspaceID uint   `json:"workspace_id"`
	ConfigKey   string `json:"config_key"`
	Alive       bool   `json:"alive"`
}
type AnonymityLevel struct {
	ID   int    `gorm:"primaryKey;autoIncrement"`
	Name string `gorm:"size:50;not null;unique"` // elite, anonymous, transparent
}

type Protocol struct {
	ID   int    `gorm:"primaryKey;autoIncrement"`
	Name string `gorm:"size:6;not null;unique"` //http, https, socks4, socks5
}
