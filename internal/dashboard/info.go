package dashboard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/evidence"
	"magpie/internal/platform/pg"

	"gorm.io/gorm"
)

type dashboardInfoCacheEntry struct {
	version     database.CheckerHealthVersion
	info        dto.DashboardInfo
	completedAt time.Time
}

var dashboardInfoCache sync.Map

func normalizeDashboardCountry(country string) string {
	trimmed := strings.TrimSpace(country)
	if trimmed == "" {
		return "Unknown"
	}

	switch strings.ToLower(trimmed) {
	case "n/a", "unknown", "unk":
		return "Unknown"
	default:
		return trimmed
	}
}

type dashboardHealthCacheEntry[T any] struct {
	version     database.CheckerHealthVersion
	value       T
	completedAt time.Time
}

func GetDashboardInfo(userid uint) dto.DashboardInfo {
	info, _ := GetDashboardInfoContext(context.Background(), userid)
	return info
}

func GetDashboardInfoContext(ctx context.Context, workspaceID uint) (dto.DashboardInfo, error) {
	value, err := getDashboardComponent(ctx, dashboardCacheKey{workspaceID: workspaceID, component: dashboardInfoComponent})
	if err != nil {
		return dto.DashboardInfo{}, err
	}
	return value.(dto.DashboardInfo), nil
}

func RefreshDashboardInfoCache(userid uint) dto.DashboardInfo {
	value, err := refreshDashboardComponent(context.Background(), dashboardCacheKey{workspaceID: userid, component: dashboardInfoComponent})
	if err != nil {
		return dto.DashboardInfo{}
	}
	return value.(dto.DashboardInfo)
}

func refreshDashboardInfoCache(db *gorm.DB, userid uint) (dto.DashboardInfo, error) {
	if db == nil {
		return dto.DashboardInfo{}, fmt.Errorf("dashboard info: database connection was not initialised")
	}
	key := dashboardCacheKey{workspaceID: userid, component: dashboardInfoComponent}
	version, err := dashboardWorkspaceVersion(db, userid)
	if err != nil {
		return dto.DashboardInfo{}, err
	}
	info, err := loadDashboardInfo(db, userid)
	if err != nil {
		return dto.DashboardInfo{}, err
	}
	currentVersion, err := dashboardWorkspaceVersion(db, userid)
	if err != nil {
		return dto.DashboardInfo{}, err
	}
	if version.Dirty || currentVersion != version {
		info.JudgeValidProxies = nil
		return info, nil
	}
	dashboardCaches.publish(key, func() {
		dashboardInfoCache.Store(userid, dashboardInfoCacheEntry{version: version, info: info, completedAt: dashboardCaches.now()})
	})
	return info, nil
}

func RefreshDashboardCaches(ctx context.Context) error {
	if pg.DB == nil {
		return fmt.Errorf("dashboard cache: database connection was not initialised")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var workspaceIDs []uint
	db := pg.DB.WithContext(ctx)
	if err := db.Model(&domain.Workspace{}).Order("id").Pluck("id", &workspaceIDs).Error; err != nil {
		return fmt.Errorf("dashboard cache: load workspace ids: %w", err)
	}

	for _, workspaceID := range workspaceIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if _, err := refreshDashboardComponent(ctx, dashboardCacheKey{workspaceID: workspaceID, component: dashboardInfoComponent}); err != nil {
				return fmt.Errorf("dashboard cache: workspace %d info: %w", workspaceID, err)
			}
			if _, err := refreshDashboardComponent(ctx, dashboardCacheKey{workspaceID: workspaceID, component: dashboardRecentComponent, limit: defaultRecentProxyChecksLimit}); err != nil {
				return fmt.Errorf("dashboard cache: workspace %d recent checks: %w", workspaceID, err)
			}
			if _, err := refreshDashboardComponent(ctx, dashboardCacheKey{workspaceID: workspaceID, component: dashboardFastestComponent, limit: dashboardFastestAliveLimit}); err != nil {
				return fmt.Errorf("dashboard cache: workspace %d fastest proxies: %w", workspaceID, err)
			}
		}
	}
	return nil
}

func loadDashboardInfo(db *gorm.DB, userid uint) (dto.DashboardInfo, error) {
	weekAgo := time.Now().AddDate(0, 0, -7)

	var (
		checks          dashboardUsageCounts
		scrapedSnapshot database.ProxySnapshotCountSummary
		inventory       dashboardInventorySummary
		judges          []judgeBreakdownRow
		wg              sync.WaitGroup
	)
	queryErrors := make(chan error, 4)
	recordError := func(label string, err error) {
		if err != nil {
			queryErrors <- fmt.Errorf("%s: %w", label, err)
		}
	}

	wg.Add(4)
	go func() {
		defer wg.Done()
		var err error
		checks, err = loadDashboardUsageCounts(db, userid, time.Now())
		recordError("check counts", err)
	}()

	go func() {
		defer wg.Done()
		var err error
		scrapedSnapshot, err = database.GetProxySnapshotCountSummaryWithDB(db, userid, domain.ProxySnapshotMetricScraped, weekAgo)
		recordError("scraped snapshot", err)
	}()

	go func() {
		defer wg.Done()
		var err error
		inventory, err = loadDashboardInventorySummary(db, userid)
		recordError("inventory summary", err)
	}()

	go func() {
		defer wg.Done()
		var err error
		judges, err = queryJudgeBreakdown(db, userid)
		recordError("judge anonymity", err)
	}()

	wg.Wait()
	close(queryErrors)
	var loadErrors []error
	for err := range queryErrors {
		loadErrors = append(loadErrors, err)
	}
	if len(loadErrors) > 0 {
		return dto.DashboardInfo{}, errors.Join(loadErrors...)
	}

	var info dto.DashboardInfo
	info.TotalChecks = checks.TotalChecks
	// Deprecated field kept until the frontend moves to dashboardView: it now
	// carries the current allowance period's checks.
	info.TotalChecksWeek = checks.ChecksThisMonth

	if scrapedSnapshot.Found {
		info.TotalScraped = scrapedSnapshot.Current
		info.TotalScrapedWeek = scrapedSnapshot.Increase
	} else {
		counts, err := database.ScrapedProxyCountByWorkspace(db, []uint{userid})
		if err != nil {
			return dto.DashboardInfo{}, fmt.Errorf("current scraped count: %w", err)
		}
		info.TotalScraped = counts[userid]
	}

	for _, row := range inventory.Countries {
		info.CountryBreakdown = append(info.CountryBreakdown, struct {
			Country string `json:"country"`
			Count   uint   `json:"count"`
		}{
			Country: normalizeDashboardCountry(row.Country),
			Count:   row.Count,
		})
	}

	for _, row := range judges {
		info.JudgeValidProxies = append(info.JudgeValidProxies, struct {
			JudgeUrl           string `json:"judge_url"`
			EliteProxies       uint   `json:"elite_proxies"`
			AnonymousProxies   uint   `json:"anonymous_proxies"`
			TransparentProxies uint   `json:"transparent_proxies"`
		}{
			JudgeUrl:           row.JudgeUrl,
			EliteProxies:       row.EliteProxies,
			AnonymousProxies:   row.AnonymousProxies,
			TransparentProxies: row.TransparentProxies,
		})
	}

	for _, row := range inventory.Reputations {
		switch row.Label {
		case "good":
			info.ReputationBreakdown.Good = row.Count
		case "neutral":
			info.ReputationBreakdown.Neutral = row.Count
		case "poor":
			info.ReputationBreakdown.Poor = row.Count
		default:
			info.ReputationBreakdown.Unknown += row.Count
		}
	}

	if inventory.TopProxyFound {
		topRow := inventory.TopProxy
		ip := topRow.IP

		info.TopReputationProxy = &struct {
			ProxyID uint64  `json:"proxy_id"`
			IP      string  `json:"ip"`
			Port    uint16  `json:"port"`
			Score   float32 `json:"score"`
			Label   string  `json:"label"`
		}{
			ProxyID: topRow.ProxyID,
			IP:      ip,
			Port:    uint16(topRow.Port),
			Score:   topRow.Score,
			Label:   topRow.Label,
		}
	}

	return info, nil
}

type judgeBreakdownRow struct {
	JudgeUrl           string `json:"judge_url"`
	EliteProxies       uint   `json:"elite_proxies"`
	AnonymousProxies   uint   `json:"anonymous_proxies"`
	TransparentProxies uint   `json:"transparent_proxies"`
}

// queryJudgeBreakdown counts the workspace's alive current evidence per
// assigned judge and anonymity level.
func queryJudgeBreakdown(db *gorm.DB, workspaceID uint) ([]judgeBreakdownRow, error) {
	var judges []judgeBreakdownRow
	err := evidence.DashboardLatestStatistics(db, workspaceID).
		Select(
			"j.full_string AS judge_url, "+
				"SUM(CASE WHEN al.name = 'elite' THEN 1 ELSE 0 END)       AS elite_proxies, "+
				"SUM(CASE WHEN al.name = 'anonymous' THEN 1 ELSE 0 END)   AS anonymous_proxies, "+
				"SUM(CASE WHEN al.name = 'transparent' THEN 1 ELSE 0 END) AS transparent_proxies",
		).
		Joins("JOIN user_judges uj ON uj.judge_id = pls.judge_id AND uj.workspace_id = ?", workspaceID).
		Joins("JOIN judges j ON j.id = pls.judge_id").
		Joins("JOIN anonymity_levels al ON al.id = pls.level_id").
		Where("pls.alive = TRUE").
		Group("j.id, j.full_string").
		Scan(&judges).Error
	return judges, err
}
