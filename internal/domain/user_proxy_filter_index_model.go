package domain

import "time"

// WorkspaceProxyFilterIndex is a denormalized read model for proxy list filtering.
type WorkspaceProxyFilterIndex struct {
	WorkspaceID uint   `gorm:"column:workspace_id;primaryKey;index:idx_workspace_proxy_filter_alive_latest,priority:1;index:idx_workspace_proxy_filter_country,priority:1;index:idx_workspace_proxy_filter_type,priority:1;index:idx_workspace_proxy_filter_reputation,priority:1"`
	ProxyID     uint64 `gorm:"primaryKey;index"`
	State       string `gorm:"size:16;not null;default:'active';index"`
	PauseReason string `gorm:"size:24;not null;default:''"`

	Host      string  `gorm:"column:host;type:varchar(253);index"`
	IPAddress *string `gorm:"column:ip_address;type:inet;index"`
	Port      uint16  `gorm:"not null;index"`

	Country       string `gorm:"size:56;not null;default:'N/A'"`
	CountryKey    string `gorm:"size:56;not null;default:'n/a';index:idx_workspace_proxy_filter_country,priority:2"`
	EstimatedType string `gorm:"size:20;not null;default:'N/A'"`
	TypeKey       string `gorm:"size:20;not null;default:'n/a';index:idx_workspace_proxy_filter_type,priority:2"`

	AnonymityLevel string `gorm:"size:50;not null;default:'N/A'"`
	AnonymityKey   string `gorm:"size:50;not null;default:'n/a';index"`

	Alive        bool      `gorm:"not null;default:false;index:idx_workspace_proxy_filter_alive_latest,priority:2"`
	LatestCheck  time.Time `gorm:"index:idx_workspace_proxy_filter_alive_latest,priority:3,sort:desc"`
	ResponseTime uint16    `gorm:"not null;default:0;index"`
	Attempt      uint8     `gorm:"not null;default:0"`

	HealthOverall *float32
	HealthHTTP    *float32
	HealthHTTPS   *float32 `gorm:"column:health_https"`
	HealthSOCKS4  *float32
	HealthSOCKS5  *float32

	AliveHTTP   bool `gorm:"not null;default:false;index"`
	AliveHTTPS  bool `gorm:"column:alive_https;not null;default:false;index"`
	AliveSOCKS4 bool `gorm:"not null;default:false;index"`
	AliveSOCKS5 bool `gorm:"not null;default:false;index"`

	ReputationLabel string   `gorm:"size:16;not null;default:'unknown';index:idx_workspace_proxy_filter_reputation,priority:2"`
	ReputationScore *float32 `gorm:"index:idx_workspace_proxy_filter_reputation,priority:3,sort:desc"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (WorkspaceProxyFilterIndex) TableName() string {
	return "user_proxy_filter_indexes"
}

type UserProxyFilterIndex = WorkspaceProxyFilterIndex
