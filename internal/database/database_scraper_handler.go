package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"magpie/internal/api/dto"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"magpie/internal/domain"
	"magpie/internal/evidence"
	"magpie/internal/platform/pg"
	"magpie/internal/readmodel"
)

func AssociateProxiesToScrapeSite(siteID uint64, proxies []domain.Proxy) error {
	if len(proxies) == 0 {
		return nil
	}

	// ProxyScrapeSite inserts touch proxy_id, scrape_site_id and created_at columns.
	const paramsPerRow = 3
	chunkSize := MaxParamsPerBatch / paramsPerRow
	if chunkSize > len(proxies) {
		chunkSize = len(proxies)
	}

	return pg.DB.Transaction(func(tx *gorm.DB) error {
		for start := 0; start < len(proxies); start += chunkSize {
			end := start + chunkSize
			if end > len(proxies) {
				end = len(proxies)
			}

			assoc := make([]domain.ProxyScrapeSite, 0, end-start)
			for _, p := range proxies[start:end] {
				if p.ID == 0 {
					continue
				}
				assoc = append(assoc, domain.ProxyScrapeSite{
					ProxyID:      p.ID,
					ScrapeSiteID: siteID,
				})
			}

			if len(assoc) == 0 {
				continue
			}

			if err := tx.
				Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "proxy_id"}, {Name: "scrape_site_id"}},
					DoNothing: true,
				}).
				Create(&assoc).Error; err != nil {
				return err
			}
		}

		return readmodel.RefreshWorkspaceScrapeSourceStatsForSites(tx, []uint64{siteID})
	})
}

type scrapeSiteAggregateRow struct {
	ProxyCount       uint            `gorm:"column:proxy_count"`
	AliveCount       uint            `gorm:"column:alive_count"`
	DeadCount        uint            `gorm:"column:dead_count"`
	UnknownCount     uint            `gorm:"column:unknown_count"`
	AvgReputation    sql.NullFloat64 `gorm:"column:avg_reputation"`
	LastProxyAddedAt sql.NullTime    `gorm:"column:last_proxy_added_at"`
	LastCheckedAt    sql.NullTime    `gorm:"column:last_checked_at"`
}

func GetScrapeSiteDetail(userId uint, scrapeSiteId uint64) (*dto.ScrapeSiteDetail, error) {
	if scrapeSiteId == 0 {
		return nil, nil
	}

	type scrapeSiteBaseRow struct {
		FetchMode            string     `gorm:"column:fetch_mode"`
		LastScrapedAt        *time.Time `gorm:"column:last_scraped_at"`
		LastScrapeStatus     string     `gorm:"column:last_scrape_status"`
		LastScrapeError      string     `gorm:"column:last_scrape_error"`
		LastScrapeProxyCount int        `gorm:"column:last_scrape_proxy_count"`

		Id      uint64    `gorm:"column:id"`
		Url     string    `gorm:"column:url"`
		AddedAt time.Time `gorm:"column:added_at"`
	}

	var base scrapeSiteBaseRow
	baseResult := pg.DB.Model(&domain.ScrapeSite{}).
		Select(
			"scrape_sites.id AS id, "+
				"scrape_sites.url AS url, "+
				"uss.created_at AS added_at, uss.fetch_mode, uss.last_scraped_at, uss.last_scrape_status, uss.last_scrape_error, uss.last_scrape_proxy_count",
		).
		Joins("JOIN user_scrape_site uss ON uss.scrape_site_id = scrape_sites.id AND uss.workspace_id = ?", userId).
		Where("scrape_sites.id = ?", scrapeSiteId).
		Limit(1).
		Scan(&base)
	if baseResult.Error != nil {
		return nil, baseResult.Error
	}
	if baseResult.RowsAffected == 0 {
		return nil, nil
	}

	var stats scrapeSiteAggregateRow
	statsResult := pg.DB.Table("proxy_scrape_site pss").
		Select(
			"COUNT(DISTINCT pss.proxy_id) AS proxy_count, "+
				"COALESCE(SUM(CASE WHEN pos.overall_alive IS TRUE THEN 1 ELSE 0 END), 0) AS alive_count, "+
				"COALESCE(SUM(CASE WHEN pos.overall_alive IS FALSE THEN 1 ELSE 0 END), 0) AS dead_count, "+
				"COALESCE(SUM(CASE WHEN pos.proxy_id IS NULL THEN 1 ELSE 0 END), 0) AS unknown_count, "+
				"AVG(pr.score) AS avg_reputation, "+
				"MAX(pss.created_at) AS last_proxy_added_at, "+
				"MAX(pos.last_checked_at) AS last_checked_at",
		).
		Joins("JOIN user_proxies up ON up.proxy_id = pss.proxy_id AND up.workspace_id = ?", userId).
		Joins(evidence.CurrentOverallStatusJoin(pg.DB, "LEFT", "up.workspace_id", "pss.proxy_id")).
		Joins("LEFT JOIN proxy_reputations pr ON pr.proxy_id = pss.proxy_id AND pr.kind = ?", domain.ProxyReputationKindOverall).
		Where("pss.scrape_site_id = ?", scrapeSiteId).
		Scan(&stats)
	if statsResult.Error != nil {
		return nil, statsResult.Error
	}

	type reputationCount struct {
		Label string `gorm:"column:label"`
		Count uint   `gorm:"column:count"`
	}

	var repCounts []reputationCount
	repResult := pg.DB.Table("proxy_scrape_site pss").
		Select("LOWER(COALESCE(NULLIF(pr.label, ''), 'unknown')) AS label, COUNT(*) AS count").
		Joins("JOIN user_proxies up ON up.proxy_id = pss.proxy_id AND up.workspace_id = ?", userId).
		Joins("LEFT JOIN proxy_reputations pr ON pr.proxy_id = pss.proxy_id AND pr.kind = ?", domain.ProxyReputationKindOverall).
		Where("pss.scrape_site_id = ?", scrapeSiteId).
		Group("label").
		Scan(&repCounts)
	if repResult.Error != nil {
		return nil, repResult.Error
	}

	breakdown := dto.ScrapeSiteReputationBreakdown{}
	for _, row := range repCounts {
		switch row.Label {
		case "good":
			breakdown.Good = row.Count
		case "neutral":
			breakdown.Neutral = row.Count
		case "poor":
			breakdown.Poor = row.Count
		default:
			breakdown.Unknown += row.Count
		}
	}

	var avgReputation *float32
	if stats.AvgReputation.Valid {
		avgReputation = new(float32(stats.AvgReputation.Float64))
	}

	var lastProxyAddedAt *time.Time
	if stats.LastProxyAddedAt.Valid {
		lastProxyAddedAt = new(stats.LastProxyAddedAt.Time)
	}

	var lastCheckedAt *time.Time
	if stats.LastCheckedAt.Valid {
		lastCheckedAt = new(stats.LastCheckedAt.Time)
	}

	autoTags, err := GetScrapeSourceTags(userId, scrapeSiteId)
	if err != nil {
		return nil, err
	}
	detail := &dto.ScrapeSiteDetail{
		AutoTags:             autoTags,
		FetchMode:            base.FetchMode,
		LastScrapedAt:        base.LastScrapedAt,
		LastScrapeStatus:     base.LastScrapeStatus,
		LastScrapeError:      base.LastScrapeError,
		LastScrapeProxyCount: base.LastScrapeProxyCount,
		Id:                   base.Id,
		Url:                  base.Url,
		AddedAt:              base.AddedAt,
		ProxyCount:           stats.ProxyCount,
		AliveCount:           stats.AliveCount,
		DeadCount:            stats.DeadCount,
		UnknownCount:         stats.UnknownCount,
		AvgReputation:        avgReputation,
		LastProxyAddedAt:     lastProxyAddedAt,
		LastCheckedAt:        lastCheckedAt,
		ReputationBreakdown:  breakdown,
	}

	return detail, nil
}

func GetScrapeSiteProxyPage(userId uint, scrapeSiteId uint64, page int, pageSize int, search string, filters dto.ProxyListFilters) ([]dto.ProxyInfo, int64, error) {
	return GetScrapeSiteProxyPageWithOptions(userId, scrapeSiteId, page, pageSize, search, filters, ProxyPageQueryOptions{
		IncludeHealth:     true,
		IncludeReputation: true,
	})
}

func GetScrapeSiteProxyPageWithOptions(userId uint, scrapeSiteId uint64, page int, pageSize int, search string, filters dto.ProxyListFilters, options ProxyPageQueryOptions) ([]dto.ProxyInfo, int64, error) {
	if scrapeSiteId == 0 {
		return []dto.ProxyInfo{}, 0, nil
	}
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 || pageSize > maxProxiesPerPage {
		pageSize = proxiesPerPage
	}

	options = normalizeProxyPageQueryOptions(options)

	query := pg.DB.Table("user_proxy_filter_indexes ufi").
		Select(
			"ufi.proxy_id AS id, "+
				"ufi.state AS state, "+
				"ufi.pause_reason AS pause_reason, "+
				"ufi.host AS ip_address, "+
				"ufi.port AS port, "+
				"ufi.estimated_type AS estimated_type, "+
				"CASE WHEN cw.checker_dirty THEN 0 ELSE ufi.response_time END AS response_time, "+
				"ufi.country AS country, "+
				"CASE WHEN cw.checker_dirty THEN 'N/A' ELSE ufi.anonymity_level END AS anonymity_level, "+
				"CASE WHEN cw.checker_dirty THEN FALSE ELSE ufi.alive END AS alive, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_overall END AS health_overall, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_http END AS health_http, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_https END AS health_https, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_socks4 END AS health_socks4, "+
				"CASE WHEN cw.checker_dirty THEN NULL ELSE ufi.health_socks5 END AS health_socks5, "+
				"ufi.latest_check AS latest_check, cw.checker_dirty AS checker_pending",
		).
		Joins("JOIN workspaces cw ON cw.id = ufi.workspace_id").
		Where("ufi.workspace_id = ?", userId).
		Joins("JOIN proxy_scrape_site pss ON pss.proxy_id = ufi.proxy_id AND pss.scrape_site_id = ?", scrapeSiteId).
		Joins("JOIN user_scrape_site uss ON uss.scrape_site_id = pss.scrape_site_id AND uss.workspace_id = ?", userId)

	query = applyProxyPageSort(query, options)

	filterQuery := buildProxyListFilterQuery(userId, filters)
	if filterQuery != nil {
		query = query.Where("ufi.proxy_id IN (?)", filterQuery)
	}

	rows := make([]dto.ProxyInfoRow, 0)
	normalizedSearch := strings.TrimSpace(search)

	if normalizedSearch == "" {
		offset := (page - 1) * pageSize
		query = query.Offset(offset).Limit(pageSize)
		if err := query.Scan(&rows).Error; err != nil {
			return []dto.ProxyInfo{}, 0, err
		}

		proxies := proxyInfoRowsToDTO(rows)
		if options.IncludeReputation {
			attachReputationsToProxyInfos(proxies)
		}
		if err := AttachProxyTagsToInfos(userId, proxies); err != nil {
			return []dto.ProxyInfo{}, 0, err
		}

		var total int64
		countQuery := pg.DB.Table("user_proxy_filter_indexes ufi").
			Where("ufi.workspace_id = ?", userId).
			Joins("JOIN proxy_scrape_site pss ON pss.proxy_id = ufi.proxy_id AND pss.scrape_site_id = ?", scrapeSiteId).
			Joins("JOIN user_scrape_site uss ON uss.scrape_site_id = pss.scrape_site_id AND uss.workspace_id = ?", userId)
		if filterQuery != nil {
			countQuery = countQuery.Where("ufi.proxy_id IN (?)", filterQuery)
		}
		if err := countQuery.Distinct("ufi.proxy_id").Count(&total).Error; err != nil {
			return proxies, 0, err
		}

		return proxies, total, nil
	}

	if err := query.Scan(&rows).Error; err != nil {
		return []dto.ProxyInfo{}, 0, err
	}

	proxies := proxyInfoRowsToDTO(rows)
	if options.IncludeReputation {
		attachReputationsToProxyInfos(proxies)
	}
	if err := AttachProxyTagsToInfos(userId, proxies); err != nil {
		return []dto.ProxyInfo{}, 0, err
	}
	filtered := filterProxiesBySearch(proxies, normalizedSearch)
	total := int64(len(filtered))
	start := (page - 1) * pageSize
	if start >= len(filtered) {
		return []dto.ProxyInfo{}, total, nil
	}

	end := start + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}

	return filtered[start:end], total, nil
}

func DeleteOrphanProxyScrapeSiteRelations(ctx context.Context) (int64, error) {
	if pg.DB == nil {
		return 0, fmt.Errorf("database not initialised")
	}

	db := pg.DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	if !pg.IsPostgresDialect(db) {
		result := db.
			Where(
				"NOT EXISTS (SELECT 1 FROM proxies p WHERE p.id = proxy_scrape_site.proxy_id) OR " +
					"NOT EXISTS (SELECT 1 FROM scrape_sites s WHERE s.id = proxy_scrape_site.scrape_site_id)",
			).
			Delete(&domain.ProxyScrapeSite{})
		return result.RowsAffected, result.Error
	}

	const orphanRelationBatchSize = 5_000
	var total int64
	for {
		result := db.Exec(`
			WITH orphan_proxy_ids AS (
				SELECT pss.proxy_id
				FROM proxy_scrape_site AS pss
				LEFT JOIN proxies AS p ON p.id = pss.proxy_id
				WHERE p.id IS NULL
				GROUP BY pss.proxy_id
				ORDER BY pss.proxy_id
				LIMIT ?
			)
			DELETE FROM proxy_scrape_site AS pss
			USING orphan_proxy_ids AS orphan
			WHERE pss.proxy_id = orphan.proxy_id
		`, orphanRelationBatchSize)
		if result.Error != nil {
			return total, result.Error
		}

		total += result.RowsAffected
		if result.RowsAffected == 0 {
			break
		}
	}

	for {
		result := db.Exec(`
			WITH orphan_source_ids AS (
				SELECT pss.scrape_site_id
				FROM proxy_scrape_site AS pss
				LEFT JOIN scrape_sites AS s ON s.id = pss.scrape_site_id
				WHERE s.id IS NULL
				GROUP BY pss.scrape_site_id
				ORDER BY pss.scrape_site_id
				LIMIT ?
			)
			DELETE FROM proxy_scrape_site AS pss
			USING orphan_source_ids AS orphan
			WHERE pss.scrape_site_id = orphan.scrape_site_id
		`, orphanRelationBatchSize)
		if result.Error != nil {
			return total, result.Error
		}

		total += result.RowsAffected
		if result.RowsAffected == 0 {
			return total, nil
		}
	}
}
