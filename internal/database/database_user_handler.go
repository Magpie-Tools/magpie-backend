package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"magpie/internal/api/dto"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/support"
)

type dashboardInfoCacheEntry struct {
	version     checkerHealthVersion
	info        dto.DashboardInfo
	completedAt time.Time
}

var dashboardInfoCache sync.Map

func GetUserFromId(id uint) domain.User {
	var users domain.User
	DB.Where("id = ?", id).First(&users)
	return users
}

func GetWorkspacesThatDontHaveJudges() []domain.Workspace {
	var workspaces []domain.Workspace
	DB.Where("id NOT IN (SELECT DISTINCT workspace_id FROM user_judges)").Find(&workspaces)
	return workspaces
}

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

func AddWorkspaceJudgesRelation(workspaces []domain.Workspace, judges []*domain.JudgeWithRegex) error {
	var workspaceJudges []domain.WorkspaceJudge

	for _, workspace := range workspaces {
		for _, judge := range judges {
			workspaceJudges = append(workspaceJudges, domain.WorkspaceJudge{
				WorkspaceID: workspace.ID,
				JudgeID:     judge.Judge.ID,
				Regex:       judge.Regex,
			})
		}
	}

	if len(workspaceJudges) > 0 {
		if err := DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "workspace_id"}, {Name: "judge_id"}},
			DoNothing: true,
		}).Create(&workspaceJudges).Error; err != nil {
			return err
		}
	}

	return nil
}

func GetAllWorkspaceJudgeRelations() ([]domain.WorkspaceJudge, []domain.JudgeWithRegex) {
	var workspaceJudges []domain.WorkspaceJudge
	if err := DB.Find(&workspaceJudges).Error; err != nil {
		return nil, nil
	}

	var results []struct {
		ID         uint   `gorm:"column:id"`
		FullString string `gorm:"column:full_string"`
		CreatedAt  time.Time
		Regex      string `gorm:"column:regex"`
	}

	if err := DB.Table("user_judges").
		Select("judges.id, judges.full_string, judges.created_at, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Scan(&results).Error; err != nil {
		return nil, nil
	}

	var judgesWithRegex []domain.JudgeWithRegex
	for _, result := range results {
		judge := &domain.Judge{
			ID:         result.ID,
			FullString: result.FullString,
			CreatedAt:  result.CreatedAt,
		}
		judge.SetUp()
		judgesWithRegex = append(judgesWithRegex, domain.JudgeWithRegex{
			Judge: judge,
			Regex: result.Regex,
		})
	}

	return workspaceJudges, judgesWithRegex
}

func UpdateWorkspaceSettings(workspaceID, userID uint, settings dto.UserSettings) error {
	// Wrap everything in a single transaction so either all changes
	// happen or none do.
	return DB.Transaction(func(tx *gorm.DB) error {
		var locked domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&locked, workspaceID).Error; err != nil {
			return err
		}
		if err := domain.ValidateCheckerSettings(settings.CheckerSettings); err != nil {
			return err
		}
		if settings.CheckerSettings != nil {
			ids := make([]uint64, 0, len(settings.CheckerSettings.Rules))
			for _, rule := range settings.CheckerSettings.Rules {
				ids = append(ids, rule.TagID)
			}
			if err := requireProxyTags(tx, workspaceID, ids); err != nil {
				return err
			}
		}
		if settings.ChangesWorkspaceSettings() {
			if !settings.HasField("http_protocol") {
				settings.HTTPProtocol = locked.HTTPProtocol
			}
			if !settings.HasField("https_protocol") {
				settings.HTTPSProtocol = locked.HTTPSProtocol
			}
			if !settings.HasField("socks4_protocol") {
				settings.SOCKS4Protocol = locked.SOCKS4Protocol
			}
			if !settings.HasField("socks5_protocol") {
				settings.SOCKS5Protocol = locked.SOCKS5Protocol
			}
			if !settings.HasField("timeout") {
				settings.Timeout = locked.Timeout
			}
			if !settings.HasField("retries") {
				settings.Retries = locked.Retries
			}
			if !settings.HasField("UseHttpsForSocks") {
				settings.UseHttpsForSocks = locked.UseHttpsForSocks
			}
			if !settings.HasField("transport_protocol") {
				settings.TransportProtocol = locked.TransportProtocol
			}
			if !settings.HasField("auto_remove_failing_proxies") {
				settings.AutoRemoveFailingProxies = locked.AutoRemoveFailingProxies
			}
			if !settings.HasField("auto_remove_failure_threshold") {
				settings.AutoRemoveFailureThreshold = locked.AutoRemoveFailureThreshold
			}
			transportProtocol := support.NormalizeTransportProtocol(settings.TransportProtocol)

			// Operational settings belong to the workspace. Table preferences remain
			// personal to this membership.
			updates := map[string]interface{}{
				"checker_dirty":              true,
				"HTTPProtocol":               settings.HTTPProtocol,
				"HTTPSProtocol":              settings.HTTPSProtocol,
				"SOCKS4Protocol":             settings.SOCKS4Protocol,
				"SOCKS5Protocol":             settings.SOCKS5Protocol,
				"Timeout":                    settings.Timeout,
				"Retries":                    settings.Retries,
				"UseHttpsForSocks":           settings.UseHttpsForSocks,
				"TransportProtocol":          transportProtocol,
				"AutoRemoveFailingProxies":   settings.AutoRemoveFailingProxies,
				"AutoRemoveFailureThreshold": settings.AutoRemoveFailureThreshold,
			}
			if settings.CheckerSettings != nil {
				encoded, err := json.Marshal(settings.CheckerSettings)
				if err != nil {
					return err
				}
				updates["checker_config"] = string(encoded)
				updates["HTTPProtocol"] = settings.CheckerSettings.Defaults.Enabled("http")
				updates["HTTPSProtocol"] = settings.CheckerSettings.Defaults.Enabled("https")
				updates["SOCKS4Protocol"] = settings.CheckerSettings.Defaults.Enabled("socks4")
				updates["SOCKS5Protocol"] = settings.CheckerSettings.Defaults.Enabled("socks5")
				updates["Timeout"] = settings.CheckerSettings.Defaults.Timeout
				updates["Retries"] = settings.CheckerSettings.Defaults.Retries
				updates["TransportProtocol"] = settings.CheckerSettings.Defaults.Transport
			} else if settings.ChangesLegacyCheckerDefaults() {
				var current domain.Workspace
				if err := tx.First(&current, workspaceID).Error; err != nil {
					return err
				}
				if current.CheckerConfig != nil {
					current.CheckerConfig = current.DefaultCheckerSettings()
					defaults := &current.CheckerConfig.Defaults
					defaults.Protocols = []string{}
					enabled := [4]bool{settings.HTTPProtocol, settings.HTTPSProtocol, settings.SOCKS4Protocol, settings.SOCKS5Protocol}
					for i, protocol := range domain.CheckerProtocols {
						if enabled[i] {
							defaults.Protocols = append(defaults.Protocols, protocol)
						}
					}
					if current.Timeout != settings.Timeout {
						defaults.Timeout = settings.Timeout
					}
					if current.Retries != settings.Retries {
						defaults.Retries = settings.Retries
					}
					if current.TransportProtocol != transportProtocol {
						defaults.Transport = transportProtocol
					}

					if err := domain.ValidateCheckerSettings(current.CheckerConfig); err != nil {
						return err
					}
					encoded, err := json.Marshal(current.CheckerConfig)
					if err != nil {
						return err
					}
					updates["checker_config"] = string(encoded)
				}
			}
			// Older clients omit the action. Preserve the workspace's selection.
			if settings.FailureAction != "" {
				updates["FailureAction"] = settings.FailureAction
			}
			locked.CheckerRevision++
			updates["checker_revision"] = locked.CheckerRevision
			oldSettings := locked.DefaultCheckerSettings()
			newSettings := oldSettings
			if raw, ok := updates["checker_config"].(string); ok {
				newSettings = &dto.CheckerSettings{}
				if err := json.Unmarshal([]byte(raw), newSettings); err != nil {
					return err
				}
			} else if locked.CheckerConfig == nil {
				updatedDefault := locked
				updatedDefault.HTTPProtocol = settings.HTTPProtocol
				updatedDefault.HTTPSProtocol = settings.HTTPSProtocol
				updatedDefault.SOCKS4Protocol = settings.SOCKS4Protocol
				updatedDefault.SOCKS5Protocol = settings.SOCKS5Protocol
				updatedDefault.TransportProtocol = transportProtocol
				updatedDefault.Timeout = settings.Timeout
				updatedDefault.Retries = settings.Retries
				newSettings = updatedDefault.DefaultCheckerSettings()
			}
			full := !reflect.DeepEqual(oldSettings.Defaults, newSettings.Defaults) || locked.UseHttpsForSocks != settings.UseHttpsForSocks || settings.HasField("judges")
			if full {
				updates["checker_full_revision"] = locked.CheckerRevision
			} else {
				changedTags := changedCheckerRuleTags(oldSettings.Rules, newSettings.Rules)
				if err := recordCheckerRuleChanges(tx, workspaceID, locked.CheckerRevision, changedTags); err != nil {
					return err
				}
			}
			if err := tx.Model(&domain.Workspace{}).
				Where("id = ?", workspaceID).
				Updates(updates).Error; err != nil {
				return err
			}

		}

		preference := domain.WorkspaceMemberPreference{WorkspaceID: workspaceID, UserID: userID}
		preferenceUpdates := make(map[string]any)
		if settings.HasField("proxy_list_columns") {
			preference.ProxyListColumns = domain.StringList(domain.NormalizeProxyListColumns(settings.ProxyListColumns))
			preferenceUpdates["proxy_list_columns"] = preference.ProxyListColumns
		}
		if settings.HasField("scrape_source_proxy_columns") {
			preference.ScrapeSourceProxyColumns = domain.StringList(domain.NormalizeScrapeSourceProxyColumns(settings.ScrapeSourceProxyColumns))
			preferenceUpdates["scrape_source_proxy_columns"] = preference.ScrapeSourceProxyColumns
		}
		if settings.HasField("scrape_source_list_columns") {
			preference.ScrapeSourceListColumns = domain.StringList(domain.NormalizeScrapeSourceListColumns(settings.ScrapeSourceListColumns))
			preferenceUpdates["scrape_source_list_columns"] = preference.ScrapeSourceListColumns
		}
		if len(preferenceUpdates) > 0 {
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "workspace_id"}, {Name: "user_id"}}, DoUpdates: clause.Assignments(preferenceUpdates)}).Create(&preference).Error; err != nil {
				return err
			}
		}
		if !settings.HasField("judges") {
			return nil
		}

		desiredByURL := make(map[string]string, len(settings.SimpleUserJudges))
		orderedURLs := make([]string, 0, len(settings.SimpleUserJudges))
		for _, s := range settings.SimpleUserJudges {
			url := strings.TrimSpace(s.Url)
			if url == "" {
				continue
			}
			if config.IsWebsiteBlocked(url) {
				log.Info("Skipped blocked judge for workspace", "workspace_id", workspaceID, "url", s.Url)
				continue
			}
			if _, exists := desiredByURL[url]; !exists {
				orderedURLs = append(orderedURLs, url)
			}
			// Preserve legacy behavior where the last repeated URL wins for regex.
			desiredByURL[url] = s.Regex
		}

		if len(orderedURLs) == 0 {
			if err := tx.Where("workspace_id = ?", workspaceID).Delete(&domain.WorkspaceJudge{}).Error; err != nil {
				return err
			}
			return nil
		}

		var existingJudges []domain.Judge
		if err := tx.
			Select("id", "full_string").
			Where("full_string IN ?", orderedURLs).
			Find(&existingJudges).Error; err != nil {
			return err
		}

		existingByURL := make(map[string]uint, len(existingJudges))
		for _, judge := range existingJudges {
			existingByURL[judge.FullString] = judge.ID
		}

		missingJudges := make([]domain.Judge, 0)
		for _, url := range orderedURLs {
			if _, exists := existingByURL[url]; exists {
				continue
			}
			missingJudges = append(missingJudges, domain.Judge{FullString: url})
		}

		if len(missingJudges) > 0 {
			if err := tx.
				Clauses(clause.OnConflict{DoNothing: true}).
				Create(&missingJudges).Error; err != nil {
				return err
			}
		}

		var resolvedJudges []domain.Judge
		if err := tx.
			Select("id", "full_string").
			Where("full_string IN ?", orderedURLs).
			Find(&resolvedJudges).Error; err != nil {
			return err
		}

		judgeIDsByURL := make(map[string]uint, len(resolvedJudges))
		for _, judge := range resolvedJudges {
			judgeIDsByURL[judge.FullString] = judge.ID
		}

		keepIDs := make([]uint, 0, len(orderedURLs))
		workspaceJudges := make([]domain.WorkspaceJudge, 0, len(orderedURLs))
		for _, url := range orderedURLs {
			judgeID, ok := judgeIDsByURL[url]
			if !ok || judgeID == 0 {
				continue
			}
			keepIDs = append(keepIDs, judgeID)
			workspaceJudges = append(workspaceJudges, domain.WorkspaceJudge{
				WorkspaceID: workspaceID,
				JudgeID:     judgeID,
				Regex:       desiredByURL[url],
			})
		}

		if len(workspaceJudges) > 0 {
			if err := tx.
				Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "workspace_id"}, {Name: "judge_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"regex"}),
				}).
				CreateInBatches(&workspaceJudges, 200).Error; err != nil {
				return err
			}
		}

		if len(keepIDs) == 0 {
			if err := tx.Where("workspace_id = ?", workspaceID).Delete(&domain.WorkspaceJudge{}).Error; err != nil {
				return err
			}
		} else {
			if err := tx.
				Where("workspace_id = ? AND judge_id NOT IN ?", workspaceID, keepIDs).
				Delete(&domain.WorkspaceJudge{}).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

// UpdateUserSettings preserves the old Go API by updating the user's default
// workspace. HTTP handlers use UpdateWorkspaceSettings directly.
func UpdateUserSettings(userID uint, settings dto.UserSettings) error {
	access, err := ResolveWorkspaceAccess(userID, 0)
	if err != nil {
		return err
	}
	return UpdateWorkspaceSettings(access.WorkspaceID, userID, settings)
}

func GetWorkspaceJudges(workspaceID uint) []dto.SimpleWorkspaceJudge {
	var results []dto.SimpleWorkspaceJudge

	if err := DB.Table("user_judges").
		Select("judges.full_string AS Url, user_judges.regex AS Regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Where("user_judges.workspace_id = ?", workspaceID).
		Scan(&results).Error; err != nil {
		return nil
	}

	return results
}

// GetUserJudges is the compatibility name for a workspace's judge settings.
// The ID identifies a workspace, not a user account.
//
// Deprecated: Use GetWorkspaceJudges.
func GetUserJudges(workspaceID uint) []dto.SimpleWorkspaceJudge {
	return GetWorkspaceJudges(workspaceID)
}

func GetWorkspaceJudgesWithRegex(workspaceID uint) ([]domain.JudgeWithRegex, error) {
	var rows []struct {
		ID         uint      `gorm:"column:id"`
		FullString string    `gorm:"column:full_string"`
		CreatedAt  time.Time `gorm:"column:created_at"`
		Regex      string    `gorm:"column:regex"`
	}

	if err := DB.Table("user_judges").
		Select("judges.id, judges.full_string, judges.created_at, user_judges.regex").
		Joins("JOIN judges ON user_judges.judge_id = judges.id").
		Where("user_judges.workspace_id = ?", workspaceID).
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]domain.JudgeWithRegex, 0, len(rows))
	for _, row := range rows {
		judge := &domain.Judge{
			ID:         row.ID,
			FullString: row.FullString,
			CreatedAt:  row.CreatedAt,
		}
		judge.SetUp()
		judge.UpdateIp()
		result = append(result, domain.JudgeWithRegex{
			Judge: judge,
			Regex: row.Regex,
		})
	}

	return result, nil
}

// GetUserJudgesWithRegex is the compatibility name for a workspace's judge assignments.
// The ID identifies a workspace, not a user account.
//
// Deprecated: Use GetWorkspaceJudgesWithRegex.
func GetUserJudgesWithRegex(workspaceID uint) ([]domain.JudgeWithRegex, error) {
	return GetWorkspaceJudgesWithRegex(workspaceID)
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
	revision := dashboardCaches.revision(key)
	version, err := currentCheckerHealthVersionWithDB(db, userid)
	if err != nil {
		return dto.DashboardInfo{}, err
	}
	// Own the delayed legacy backfill as well as the six loader queries. On
	// success keep the context live until the backfill finishes; on failure
	// cancel it and join before returning. Fast cold reads can take two seconds.
	var info dto.DashboardInfo
	if owner, _ := db.Statement.Context.Value(dashboardBackfillContextKey{}).(*dashboardBackfillWork); owner != nil {
		// Explicit all-workspace prewarm joins these children as one outer
		// group, so their existing two-second delays do not become serial.
		info, err = loadDashboardInfo(db, userid)
	} else {
		loadCtx, cancel := context.WithCancel(dashboardDBContext(db.Statement.Context))
		backfills := &dashboardBackfillWork{ctx: loadCtx}
		loadCtx = context.WithValue(loadCtx, dashboardBackfillContextKey{}, backfills)
		info, err = loadDashboardInfo(db.WithContext(loadCtx), userid)
		if err != nil {
			cancel()
		}
		backfills.Wait()
		if err == nil {
			err = loadCtx.Err()
		}
		cancel()
	}
	if err != nil {
		return dto.DashboardInfo{}, err
	}
	currentVersion, err := currentCheckerHealthVersionWithDB(db, userid)
	if err != nil {
		return dto.DashboardInfo{}, err
	}
	if version.Dirty || currentVersion != version {
		info.JudgeValidProxies = nil
		return info, nil
	}
	dashboardCaches.publish(key, revision, func() {
		dashboardInfoCache.Store(userid, dashboardInfoCacheEntry{version: version, info: info, completedAt: dashboardCaches.now()})
	})
	return info, nil
}

func RefreshDashboardCaches(ctx context.Context) (returnErr error) {
	if DB == nil {
		return fmt.Errorf("dashboard cache: database connection was not initialised")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	groupCtx, cancel := context.WithCancel(ctx)
	backfills := &dashboardBackfillWork{ctx: groupCtx}
	ctx = context.WithValue(groupCtx, dashboardBackfillContextKey{}, backfills)
	defer func() {
		if returnErr != nil {
			cancel()
		}
		backfills.Wait()
		if returnErr == nil {
			returnErr = groupCtx.Err()
		}
		cancel()
	}()

	var workspaceIDs []uint
	db := DB.WithContext(ctx)
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

	type jvp struct {
		JudgeUrl           string `json:"judge_url"`
		EliteProxies       uint   `json:"elite_proxies"`
		AnonymousProxies   uint   `json:"anonymous_proxies"`
		TransparentProxies uint   `json:"transparent_proxies"`
	}
	var (
		checks          dashboardCheckCounts
		scrapedSnapshot proxySnapshotCountSummary
		inventory       dashboardInventorySummary
		judges          []jvp
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
		checks, err = loadDashboardCheckCountsWithDB(db, userid, weekAgo)
		if err == nil {
			return
		}

		log.Error("dashboard: failed to load pre-aggregated check counts", "user_id", userid, "error", err)
		if db.Statement.Context.Err() != nil {
			recordError("check counts", db.Statement.Context.Err())
			return
		}
		checks, err = loadDashboardCheckCountsDirectWithDB(db, userid, weekAgo)
		if err != nil {
			log.Error("dashboard: failed to load fallback check counts", "user_id", userid, "error", err)
		}
		recordError("check counts", err)
	}()

	go func() {
		defer wg.Done()
		var err error
		scrapedSnapshot, err = getProxySnapshotCountSummaryWithDB(db, userid, domain.ProxySnapshotMetricScraped, weekAgo)
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
		err := dashboardLatestStatistics(db, userid).
			Select(
				"j.full_string AS judge_url, "+
					"SUM(CASE WHEN al.name = 'elite' THEN 1 ELSE 0 END)       AS elite_proxies, "+
					"SUM(CASE WHEN al.name = 'anonymous' THEN 1 ELSE 0 END)   AS anonymous_proxies, "+
					"SUM(CASE WHEN al.name = 'transparent' THEN 1 ELSE 0 END) AS transparent_proxies",
			).
			Joins("JOIN user_judges uj ON uj.judge_id = pls.judge_id AND uj.workspace_id = ?", userid).
			Joins("JOIN judges j ON j.id = pls.judge_id").
			Joins("JOIN anonymity_levels al ON al.id = pls.level_id").
			Where("pls.alive = TRUE").
			Group("j.id, j.full_string").
			Scan(&judges).Error
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
	info.TotalChecksWeek = checks.TotalChecksWeek

	if scrapedSnapshot.Found {
		info.TotalScraped = scrapedSnapshot.Current
		info.TotalScrapedWeek = scrapedSnapshot.Increase
	} else {
		counts, err := scrapedProxyCountByWorkspace(db, []uint{userid})
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

func ChangePassword(userID uint, password string) error {
	err := DB.Model(&domain.User{}).Where("ID = ?", userID).Update("password", password).Error
	return err
}

func ChangePasswordIfCurrent(userID uint, expectedPassword string, newPassword string) (bool, error) {
	result := DB.Model(&domain.User{}).
		Where("ID = ? AND password = ?", userID, expectedPassword).
		Update("password", newPassword)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func changedCheckerRuleTags(before, after []dto.TagCheckerRule) []uint64 {
	type indexedRule struct {
		index int
		rule  dto.TagCheckerRule
	}
	old := make(map[uint64]indexedRule, len(before))
	for i, rule := range before {
		old[rule.TagID] = indexedRule{i, rule}
	}
	ids := make([]uint64, 0)
	for i, rule := range after {
		prior, exists := old[rule.TagID]
		if !exists || prior.index != i || !reflect.DeepEqual(prior.rule, rule) {
			ids = append(ids, rule.TagID)
		}
		delete(old, rule.TagID)
	}
	for id := range old {
		ids = append(ids, id)
	}
	return ids
}
