package domain

// Only protocols changed by matching checker rules need a materialized override.
// Workspaces hold default keys, avoiding rows for every untagged proxy route.
type ProxyCheckerPlan struct {
	WorkspaceID uint   `gorm:"primaryKey"`
	ProxyID     uint64 `gorm:"primaryKey"`
	ProtocolID  int    `gorm:"primaryKey"`
	ConfigKey   string `gorm:"not null;size:64"`
	Transport   string `gorm:"not null;size:8"`
}
