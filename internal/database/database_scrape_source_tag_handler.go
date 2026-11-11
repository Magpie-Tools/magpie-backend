package database

import (
	"magpie/internal/api/dto"
	"magpie/internal/platform/pg"
)

func GetScrapeSourceTags(workspaceID uint, siteID uint64) ([]dto.ProxyTag, error) {
	tags := []dto.ProxyTag{}
	err := pg.DB.Table("scrape_source_tags rule").
		Select("tag.id, tag.name, tag.color").
		Joins("JOIN proxy_tags tag ON tag.id = rule.proxy_tag_id AND tag.workspace_id = rule.workspace_id").
		Where("rule.workspace_id = ? AND rule.scrape_site_id = ?", workspaceID, siteID).
		Order("tag.name_key, tag.id").Scan(&tags).Error
	return tags, err
}
