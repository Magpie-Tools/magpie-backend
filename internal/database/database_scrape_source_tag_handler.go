package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const scrapeTagBatchSize = 500

func GetScrapeSourceTags(workspaceID uint, siteID uint64) ([]dto.ProxyTag, error) {
	tags := []dto.ProxyTag{}
	err := DB.Table("scrape_source_tags rule").
		Select("tag.id, tag.name, tag.color").
		Joins("JOIN proxy_tags tag ON tag.id = rule.proxy_tag_id AND tag.workspace_id = rule.workspace_id").
		Where("rule.workspace_id = ? AND rule.scrape_site_id = ?", workspaceID, siteID).
		Order("tag.name_key, tag.id").Scan(&tags).Error
	return tags, err
}

func validateScrapeSourceTags(tx *gorm.DB, workspaceID uint, tagIDs []uint64) error {
	for _, id := range tagIDs {
		if id == 0 {
			return ErrProxyTagNotFound
		}
	}
	for start := 0; start < len(tagIDs); start += scrapeTagBatchSize {
		if err := requireProxyTags(tx, workspaceID, tagIDs[start:min(start+scrapeTagBatchSize, len(tagIDs))]); err != nil {
			return err
		}
	}
	return nil
}

func createScrapeSourceTags(tx *gorm.DB, workspaceID uint, siteIDs, tagIDs []uint64) error {
	if len(siteIDs) == 0 || len(tagIDs) == 0 {
		return nil
	}
	rules := make([]domain.ScrapeSourceTag, 0, scrapeTagBatchSize)
	flush := func() error {
		if len(rules) == 0 {
			return nil
		}
		err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&rules).Error
		rules = rules[:0]
		return err
	}
	for _, siteID := range siteIDs {
		for _, tagID := range tagIDs {
			rules = append(rules, domain.ScrapeSourceTag{WorkspaceID: workspaceID, ScrapeSiteID: siteID, ProxyTagID: tagID})
			if len(rules) == scrapeTagBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// UpdateScrapeSourceSettings changes only supplied settings and never touches
// existing proxy tag assignments or historical scrape results.
func UpdateScrapeSourceSettings(ctx context.Context, workspaceID uint, siteID uint64, settings dto.ScrapeSourceSettings) (bool, error) {
	if settings.FetchMode == nil && settings.AutoTagIDs == nil {
		return false, fmt.Errorf("no source settings supplied")
	}
	if settings.FetchMode != nil && !domain.ValidScrapeFetchMode(*settings.FetchMode) {
		return false, fmt.Errorf("invalid fetch mode")
	}
	found := false
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var source domain.WorkspaceScrapeSite
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("workspace_id = ? AND scrape_site_id = ?", workspaceID, siteID).First(&source).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if settings.AutoTagIDs != nil {
			for _, id := range *settings.AutoTagIDs {
				if id == 0 {
					return ErrProxyTagNotFound
				}
			}
			tagIDs := normalizeUint64IDs(*settings.AutoTagIDs)
			if err := validateScrapeSourceTags(tx, workspaceID, tagIDs); err != nil {
				return err
			}
			if err := tx.Where("workspace_id = ? AND scrape_site_id = ?", workspaceID, siteID).Delete(&domain.ScrapeSourceTag{}).Error; err != nil {
				return err
			}
			if err := createScrapeSourceTags(tx, workspaceID, []uint64{siteID}, tagIDs); err != nil {
				return err
			}
		}
		if settings.FetchMode != nil && source.FetchMode != *settings.FetchMode {
			if err := tx.Model(&domain.WorkspaceScrapeSite{}).
				Where("workspace_id = ? AND scrape_site_id = ?", workspaceID, siteID).
				Updates(map[string]any{"fetch_mode": *settings.FetchMode, "last_scraped_at": nil, "last_scrape_status": "", "last_scrape_error": "", "last_scrape_proxy_count": 0}).Error; err != nil {
				return err
			}
		}
		found = true
		return nil
	})
	return found, err
}

// ApplyScrapeSourceTags uses live rules and subscriptions, restricted to the
// workspaces that participated in this fetch. Proxy.Workspaces includes other
// owners and must not be used to select recipients. No health/state filter is
// needed: tags classify discovery, including paused and archived proxies.
func ApplyScrapeSourceTags(ctx context.Context, siteID uint64, mode string, workspaceIDs []uint, proxies []domain.Proxy) error {
	if len(workspaceIDs) == 0 || len(proxies) == 0 {
		return nil
	}
	if !domain.ValidScrapeFetchMode(mode) {
		return fmt.Errorf("invalid fetch mode")
	}
	ids := make([]uint64, 0, len(proxies))
	for _, proxy := range proxies {
		ids = append(ids, proxy.ID)
	}
	ids = normalizeUint64IDs(ids)
	checkerChanged := make(map[uint]bool)
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for w := 0; w < len(workspaceIDs); w += scrapeTagBatchSize {
			workspaces := workspaceIDs[w:min(w+scrapeTagBatchSize, len(workspaceIDs))]
			for p := 0; p < len(ids); p += scrapeTagBatchSize {
				proxyIDs := ids[p:min(p+scrapeTagBatchSize, len(ids))]
				if err := tx.Exec(`
					INSERT INTO proxy_tag_assignments (workspace_id, proxy_id, proxy_tag_id, created_at)
					SELECT rule.workspace_id, managed.proxy_id, rule.proxy_tag_id, ?
					FROM scrape_source_tags rule
					JOIN user_scrape_site source ON source.workspace_id = rule.workspace_id AND source.scrape_site_id = rule.scrape_site_id
					JOIN proxy_tags tag ON tag.id = rule.proxy_tag_id AND tag.workspace_id = rule.workspace_id
					JOIN user_proxies managed ON managed.workspace_id = rule.workspace_id
					WHERE rule.scrape_site_id = ? AND source.fetch_mode = ?
					  AND rule.workspace_id IN ? AND managed.proxy_id IN ?
					ON CONFLICT (workspace_id, proxy_id, proxy_tag_id) DO NOTHING
				`, time.Now().UTC(), siteID, mode, workspaces, proxyIDs).Error; err != nil {
					return err
				}
			}
		}
		for _, workspaceID := range workspaceIDs {
			var tagIDs []uint64
			if err := tx.Model(&domain.ScrapeSourceTag{}).Where("workspace_id = ? AND scrape_site_id = ?", workspaceID, siteID).Pluck("proxy_tag_id", &tagIDs).Error; err != nil {
				return err
			}
			changed, err := markCheckerAssignmentsDirty(tx, workspaceID, tagIDs, ids)
			if err != nil {
				return err
			}
			checkerChanged[workspaceID] = changed
		}
		return nil
	})
	if err == nil {
		for _, workspaceID := range workspaceIDs {
			if checkerChanged[workspaceID] {
				checkerconfig.TagAssignmentsChanged(workspaceID)
			}
		}
	}
	return err
}
