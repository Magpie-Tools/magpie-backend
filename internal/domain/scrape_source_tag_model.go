package domain

// ScrapeSourceTag configures an additive tag for one workspace's source.
// Removing a rule never removes the resulting ProxyTagAssignments.
type ScrapeSourceTag struct {
	WorkspaceID  uint   `gorm:"primaryKey"`
	ScrapeSiteID uint64 `gorm:"primaryKey"`
	ProxyTagID   uint64 `gorm:"primaryKey;index"`

	Source WorkspaceScrapeSite `gorm:"belongsTo:Source;foreignKey:WorkspaceID,ScrapeSiteID;references:WorkspaceID,ScrapeSiteID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Tag    ProxyTag            `gorm:"belongsTo:Tag;foreignKey:ProxyTagID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}
