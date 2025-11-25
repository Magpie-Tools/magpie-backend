package database

import (
	"context"
	"errors"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

type UserJudgeAssignment struct {
	JudgeID    uint
	FullString string
	Regex      string
	CreatedAt  time.Time
}

type WebsiteBlacklistCleanupResult struct {
	JudgeRelationsRemoved  int64
	ScrapeRelationsRemoved int64
	BlockedScrapeSites     []domain.ScrapeSite
	UpdatedUserJudges      map[uint][]UserJudgeAssignment
}

// RemoveBlockedWebsitesFromUsers deletes user associations to judges and scrape sites that
// now resolve to blocked hosts. It returns enough detail for callers to refresh caches and queues.
func RemoveBlockedWebsitesFromUsers(ctx context.Context, blockedWebsites []string) (*WebsiteBlacklistCleanupResult, error) {
	if DB == nil {
		return nil, errors.New("database not initialised")
	}

	normalized := config.NormalizeWebsiteBlacklist(blockedWebsites)
	blockedSet := config.NewWebsiteBlocklistSet(normalized)
	if len(blockedSet) == 0 {
		return &WebsiteBlacklistCleanupResult{}, nil
	}

	result := &WebsiteBlacklistCleanupResult{}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	judgeAffectedUsers := make(map[uint]struct{})

	err := db.Transaction(func(tx *gorm.DB) error {
		blockedJudgeIDs, affectedUsers, err := findBlockedJudgeIDs(tx, blockedSet)
		if err != nil {
			return err
		}
		for uid := range affectedUsers {
			judgeAffectedUsers[uid] = struct{}{}
		}
		if len(blockedJudgeIDs) > 0 {
			if err := deleteUserJudgeRelations(tx, blockedJudgeIDs, &result.JudgeRelationsRemoved); err != nil {
				return err
			}
		}

		blockedSites, err := findBlockedScrapeSites(tx, blockedSet)
		if err != nil {
			return err
		}
		if len(blockedSites.siteIDs) > 0 {
			result.BlockedScrapeSites = blockedSites.sites
			if err := deleteUserScrapeRelations(tx, blockedSites.siteIDs, &result.ScrapeRelationsRemoved); err != nil {
				return err
			}
		}

		if len(judgeAffectedUsers) > 0 {
			updates, err := loadRemainingUserJudges(tx, judgeAffectedUsers)
			if err != nil {
				return err
			}
			result.UpdatedUserJudges = updates
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func findBlockedJudgeIDs(tx *gorm.DB, blockedSet map[string]struct{}) ([]uint, map[uint]struct{}, error) {
	var rows []struct {
		UserID  uint
		JudgeID uint
		URL     string
	}

	if err := tx.Table("user_judges").
		Select("user_judges.user_id, user_judges.judge_id, judges.full_string AS url").
		Joins("JOIN judges ON judges.id = user_judges.judge_id").
		Find(&rows).Error; err != nil {
		return nil, nil, err
	}

	blocked := make(map[uint]struct{}, len(rows))
	affectedUsers := make(map[uint]struct{})

	for _, row := range rows {
		if !config.IsWebsiteBlockedForSet(row.URL, blockedSet) {
			continue
		}
		blocked[row.JudgeID] = struct{}{}
		affectedUsers[row.UserID] = struct{}{}
	}

	return mapKeysUint(blocked), affectedUsers, nil
}

type blockedScrapeSites struct {
	siteIDs []uint64
	sites   []domain.ScrapeSite
}

func findBlockedScrapeSites(tx *gorm.DB, blockedSet map[string]struct{}) (blockedScrapeSites, error) {
	var rows []struct {
		SiteID uint64
		URL    string
	}

	if err := tx.Table("user_scrape_site").
		Select("DISTINCT scrape_sites.id AS site_id, scrape_sites.url").
		Joins("JOIN scrape_sites ON scrape_sites.id = user_scrape_site.scrape_site_id").
		Find(&rows).Error; err != nil {
		return blockedScrapeSites{}, err
	}

	blockedSiteIDs := make(map[uint64]struct{}, len(rows))
	for _, row := range rows {
		if config.IsWebsiteBlockedForSet(row.URL, blockedSet) {
			blockedSiteIDs[row.SiteID] = struct{}{}
		}
	}

	if len(blockedSiteIDs) == 0 {
		return blockedScrapeSites{}, nil
	}

	ids := mapKeysUint64(blockedSiteIDs)

	var sites []domain.ScrapeSite
	if err := tx.Where("id IN ?", ids).Find(&sites).Error; err != nil {
		return blockedScrapeSites{}, err
	}

	return blockedScrapeSites{
		siteIDs: ids,
		sites:   sites,
	}, nil
}

func deleteUserJudgeRelations(tx *gorm.DB, judgeIDs []uint, totalRemoved *int64) error {
	if len(judgeIDs) == 0 {
		return nil
	}

	chunkSize := deleteChunkSize
	if chunkSize > len(judgeIDs) {
		chunkSize = len(judgeIDs)
	}
	if chunkSize <= 0 {
		chunkSize = len(judgeIDs)
	}

	for start := 0; start < len(judgeIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(judgeIDs) {
			end = len(judgeIDs)
		}

		res := tx.
			Where("judge_id IN ?", judgeIDs[start:end]).
			Delete(&domain.UserJudge{})
		if res.Error != nil {
			return res.Error
		}
		*totalRemoved += res.RowsAffected
	}

	return nil
}

func deleteUserScrapeRelations(tx *gorm.DB, siteIDs []uint64, totalRemoved *int64) error {
	if len(siteIDs) == 0 {
		return nil
	}

	chunkSize := deleteChunkSize
	if chunkSize > len(siteIDs) {
		chunkSize = len(siteIDs)
	}
	if chunkSize <= 0 {
		chunkSize = len(siteIDs)
	}

	for start := 0; start < len(siteIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(siteIDs) {
			end = len(siteIDs)
		}

		res := tx.
			Where("scrape_site_id IN ?", siteIDs[start:end]).
			Delete(&domain.UserScrapeSite{})
		if res.Error != nil {
			return res.Error
		}
		*totalRemoved += res.RowsAffected
	}

	return nil
}

func loadRemainingUserJudges(tx *gorm.DB, userSet map[uint]struct{}) (map[uint][]UserJudgeAssignment, error) {
	userIDs := mapKeysUint(userSet)
	if len(userIDs) == 0 {
		return nil, nil
	}

	var rows []struct {
		UserID     uint
		JudgeID    uint
		FullString string
		CreatedAt  time.Time
		Regex      string
	}

	if err := tx.Table("user_judges").
		Select("user_judges.user_id, judges.id AS judge_id, judges.full_string, judges.created_at, user_judges.regex").
		Joins("JOIN judges ON judges.id = user_judges.judge_id").
		Where("user_judges.user_id IN ?", userIDs).
		Order("user_judges.user_id, judges.id").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	result := make(map[uint][]UserJudgeAssignment, len(userIDs))
	for _, id := range userIDs {
		result[id] = nil
	}
	for _, row := range rows {
		result[row.UserID] = append(result[row.UserID], UserJudgeAssignment{
			JudgeID:    row.JudgeID,
			FullString: row.FullString,
			Regex:      row.Regex,
			CreatedAt:  row.CreatedAt,
		})
	}

	return result, nil
}

func mapKeysUint(set map[uint]struct{}) []uint {
	keys := make([]uint, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}

func mapKeysUint64(set map[uint64]struct{}) []uint64 {
	keys := make([]uint64, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}
