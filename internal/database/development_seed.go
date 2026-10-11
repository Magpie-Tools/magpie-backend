package database

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/support"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DevelopmentSeedOptions struct {
	Emails              []string
	Password            string
	ProxiesPerWorkspace int
	HistoryDays         int
	Seed                int64
	Now                 time.Time
}

type DevelopmentSeedPlan struct {
	Accounts        []string `json:"accounts"`
	MissingAccounts []string `json:"missing_accounts"`
	ResetWorkspaces []uint   `json:"reset_workspaces"`
	ExistingRoutes  int64    `json:"existing_routes"`
	NewWorkspaces   int      `json:"new_workspaces"`
	NewRoutes       int      `json:"new_routes"`
}

type DevelopmentSeedResult struct {
	Workspaces     int                 `json:"workspaces"`
	Proxies        int                 `json:"proxies"`
	Statistics     int                 `json:"statistics"`
	Tags           int                 `json:"tags"`
	Sources        int                 `json:"sources"`
	Rotators       int                 `json:"rotators"`
	AlertRules     int                 `json:"alert_rules"`
	Incidents      int                 `json:"incidents"`
	HistoryDays    int                 `json:"history_days"`
	Seed           int64               `json:"seed"`
	RemovedProxies []domain.Proxy      `json:"-"`
	RefreshProxies []domain.Proxy      `json:"-"`
	RemovedSources []domain.ScrapeSite `json:"-"`
}

func (options *DevelopmentSeedOptions) normalize() error {
	if config.InProductionMode || config.RuntimeEnvironmentIndicatesProduction() {
		return errors.New("development data cannot be seeded in production")
	}
	if len(options.Emails) != 3 {
		return errors.New("supply exactly three development account emails")
	}
	seen := make(map[string]bool)
	options.Emails = append([]string(nil), options.Emails...)
	for i, email := range options.Emails {
		email = strings.ToLower(strings.TrimSpace(email))
		address, err := mail.ParseAddress(email)
		if err != nil || address.Address != email || seen[email] {
			return errors.New("development account emails must be valid and distinct")
		}
		seen[email] = true
		options.Emails[i] = email
	}
	if options.ProxiesPerWorkspace < 10 || options.ProxiesPerWorkspace > 10000 {
		return errors.New("proxies per workspace must be between 10 and 10000")
	}
	if options.HistoryDays < 1 || options.HistoryDays > 90 {
		return errors.New("history days must be between 1 and 90")
	}
	if options.Now.IsZero() {
		options.Now = time.Now()
	}
	options.Now = options.Now.UTC().Truncate(time.Minute)
	return nil
}

// PlanDevelopmentSeed reads the scope without migrating, creating accounts, or
// changing their data. Shared workspaces with other users block the whole reset.
func PlanDevelopmentSeed(ctx context.Context, db *gorm.DB, options DevelopmentSeedOptions) (DevelopmentSeedPlan, error) {
	if err := options.normalize(); err != nil {
		return DevelopmentSeedPlan{}, err
	}
	plan, _, err := developmentSeedScope(db.WithContext(ctx), options)
	return plan, err
}

func developmentSeedScope(tx *gorm.DB, options DevelopmentSeedOptions) (DevelopmentSeedPlan, []domain.User, error) {
	plan := DevelopmentSeedPlan{Accounts: options.Emails, MissingAccounts: []string{}, ResetWorkspaces: []uint{}, NewWorkspaces: 5, NewRoutes: 5 * options.ProxiesPerWorkspace}
	var users []domain.User
	if err := tx.Where("LOWER(email) IN ?", options.Emails).Find(&users).Error; err != nil {
		return plan, nil, err
	}
	ids := make([]uint, 0, len(users))
	for _, email := range options.Emails {
		found := false
		for _, user := range users {
			if strings.EqualFold(user.Email, email) {
				if found {
					return plan, nil, fmt.Errorf("multiple users match %s", email)
				}
				ids = append(ids, user.ID)
				found = true
			}
		}
		if !found {
			plan.MissingAccounts = append(plan.MissingAccounts, email)
		}
	}
	if len(ids) == 0 {
		return plan, users, nil
	}
	if err := tx.Model(&domain.WorkspaceMembership{}).Where("user_id IN ?", ids).Distinct().Order("workspace_id").Pluck("workspace_id", &plan.ResetWorkspaces).Error; err != nil {
		return plan, nil, err
	}
	if len(plan.ResetWorkspaces) == 0 {
		return plan, users, nil
	}
	var outsideMembers int64
	if err := tx.Model(&domain.WorkspaceMembership{}).Where("workspace_id IN ? AND user_id NOT IN ?", plan.ResetWorkspaces, ids).Count(&outsideMembers).Error; err != nil {
		return plan, nil, err
	}
	if outsideMembers > 0 {
		return plan, nil, errors.New("reset refused: a selected account shares a workspace with an unselected user")
	}
	var owned []uint
	if err := tx.Model(&domain.WorkspaceMembership{}).Where("workspace_id IN ? AND user_id IN ? AND role = ?", plan.ResetWorkspaces, ids, domain.WorkspaceRoleOwner).Distinct().Pluck("workspace_id", &owned).Error; err != nil {
		return plan, nil, err
	}
	if len(owned) != len(plan.ResetWorkspaces) {
		return plan, nil, errors.New("reset refused: every affected workspace must have a selected owner")
	}
	err := tx.Model(&domain.ManagedProxy{}).Where("workspace_id IN ?", plan.ResetWorkspaces).Count(&plan.ExistingRoutes).Error
	return plan, users, err
}

// SeedDevelopmentData is an offline operation. The transaction replaces only
// the selected users' workspaces and deletes only their newly orphaned routes.
// Account IDs, password hashes and global roles survive repeated resets.
func SeedDevelopmentData(ctx context.Context, db *gorm.DB, options DevelopmentSeedOptions) (DevelopmentSeedResult, error) {
	if err := options.normalize(); err != nil {
		return DevelopmentSeedResult{}, err
	}
	result := DevelopmentSeedResult{HistoryDays: options.HistoryDays, Seed: options.Seed}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() != "postgres" {
			return errors.New("development seeding requires PostgreSQL")
		}
		var locked bool
		if err := tx.Raw("SELECT pg_try_advisory_xact_lock(629741834)").Scan(&locked).Error; err != nil {
			return err
		}
		if !locked {
			return errors.New("another development data reset is running")
		}
		var lockedUsers []domain.User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("LOWER(email) IN ?", options.Emails).Find(&lockedUsers).Error; err != nil {
			return err
		}
		plan, users, err := developmentSeedScope(tx, options)
		if err != nil {
			return err
		}
		if len(plan.MissingAccounts) > 0 {
			if err := support.ValidatePassword(options.Password); err != nil {
				return fmt.Errorf("MAGPIE_DEV_PASSWORD is required for missing accounts: %w", err)
			}
			password, err := support.HashPassword(options.Password)
			if err != nil {
				return err
			}
			var admins int64
			if err := tx.Model(&domain.User{}).Where("role = ?", "admin").Count(&admins).Error; err != nil {
				return err
			}
			for _, email := range plan.MissingAccounts {
				user := domain.User{Email: email, Password: password, Role: "user", CreatedAt: options.Now}
				if admins == 0 && email == options.Emails[0] {
					user.Role = "admin"
					admins++
				}
				if err := tx.Create(&user).Error; err != nil {
					return err
				}
				users = append(users, user)
			}
		}
		// Put the users in the caller's order, independent of existing IDs.
		ordered := make([]domain.User, 0, 3)
		for _, email := range options.Emails {
			for _, user := range users {
				if strings.EqualFold(user.Email, email) {
					ordered = append(ordered, user)
				}
			}
		}
		if err := resetDevelopmentWorkspaces(tx, plan.ResetWorkspaces, &result); err != nil {
			return err
		}
		userIDs := []uint{ordered[0].ID, ordered[1].ID, ordered[2].ID}
		if err := tx.Where("user_id IN ?", userIDs).Delete(&domain.PasswordResetToken{}).Error; err != nil {
			return err
		}
		if err := tx.Where("invitee_user_id IN ? OR inviter_user_id IN ?", userIDs, userIDs).Delete(&domain.WorkspaceInvitation{}).Error; err != nil {
			return err
		}
		if err := tx.Where("LOWER(to_address) IN ?", options.Emails).Delete(&domain.EmailOutbox{}).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.User{}).Where("id IN ?", userIDs).UpdateColumns(map[string]any{
			"http_protocol": true, "http_s_protocol": true, "socks4_protocol": true, "socks5_protocol": true,
			"timeout": 7500, "retries": 2, "use_https_for_socks": true, "transport_protocol": "tcp",
			"auto_remove_failing_proxies": false, "auto_remove_failure_threshold": 3,
			"proxy_list_columns": "[]", "scrape_source_proxy_columns": "[]", "scrape_source_list_columns": "[]",
		}).Error; err != nil {
			return err
		}
		return createDevelopmentFixtures(tx, ordered, options, &result)
	})
	if err != nil {
		return DevelopmentSeedResult{}, err
	}
	return result, nil
}

func resetDevelopmentWorkspaces(tx *gorm.DB, workspaceIDs []uint, result *DevelopmentSeedResult) error {
	if len(workspaceIDs) == 0 {
		return nil
	}
	var proxyIDs []uint64
	var siteIDs []uint64
	var judgeIDs []uint
	if err := tx.Model(&domain.ManagedProxy{}).Where("workspace_id IN ?", workspaceIDs).Distinct().Pluck("proxy_id", &proxyIDs).Error; err != nil {
		return err
	}
	if err := tx.Model(&domain.WorkspaceScrapeSite{}).Where("workspace_id IN ?", workspaceIDs).Distinct().Pluck("scrape_site_id", &siteIDs).Error; err != nil {
		return err
	}
	if err := tx.Model(&domain.WorkspaceJudge{}).Where("workspace_id IN ?", workspaceIDs).Distinct().Pluck("judge_id", &judgeIDs).Error; err != nil {
		return err
	}
	models := []any{
		&domain.AlertDelivery{}, &domain.AlertIncident{}, &domain.AlertRule{}, &domain.AlertDestination{},
		&domain.ScrapeSourceTag{}, &domain.ProxyTagAssignment{}, &domain.ProxyCheckerPlan{}, &domain.CheckerProxyChange{},
		&domain.WorkspaceProxyFilterIndex{}, &domain.ManagedProxy{}, &domain.ProxyTag{},
		&domain.WorkspaceScrapeSourceStat{}, &domain.WorkspaceScrapeSite{}, &domain.WorkspaceJudge{},
		&domain.RotatingProxy{}, &domain.ProxyHistory{}, &domain.ProxySnapshot{}, &domain.WorkspaceUsagePeriod{},
		&domain.WorkspaceSubscription{}, &domain.WorkspaceInvitation{}, &domain.WorkspaceMemberPreference{}, &domain.WorkspaceMembership{},
	}
	for _, model := range models {
		if err := tx.Unscoped().Where("workspace_id IN ?", workspaceIDs).Delete(model).Error; err != nil {
			return err
		}
	}
	if err := tx.Where("id IN ?", workspaceIDs).Delete(&domain.Workspace{}).Error; err != nil {
		return err
	}
	// Batch the candidate IDs to stay below PostgreSQL's bind-parameter limit.
	for start := 0; start < len(proxyIDs); start += 1000 {
		batch := proxyIDs[start:min(start+1000, len(proxyIDs))]
		inactive, refresh, err := loadProxyQueueChangesForUint64(tx, batch)
		if err != nil {
			return err
		}
		result.RemovedProxies = append(result.RemovedProxies, inactive...)
		result.RefreshProxies = append(result.RefreshProxies, refresh...)
		var orphans []uint64
		if err := tx.Model(&domain.Proxy{}).Where("id IN ?", batch).Where("NOT EXISTS (SELECT 1 FROM user_proxies up WHERE up.proxy_id = proxies.id)").Pluck("id", &orphans).Error; err != nil {
			return err
		}
		if len(orphans) == 0 {
			continue
		}
		for _, model := range []any{&domain.ProxyDailyCheck{}, &domain.ProxyDailyCheckProxyBackfill{}, &domain.ProxyReputationRefresh{}} {
			if err := tx.Where("proxy_id IN ?", orphans).Delete(model).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("id IN ?", orphans).Delete(&domain.Proxy{}).Error; err != nil {
			return err
		}
	}
	if len(siteIDs) > 0 {
		if err := tx.Where("id IN ?", siteIDs).Where("NOT EXISTS (SELECT 1 FROM user_scrape_site us WHERE us.scrape_site_id = scrape_sites.id)").Find(&result.RemovedSources).Error; err != nil {
			return err
		}
		if err := tx.Where("id IN ?", siteIDs).Where("NOT EXISTS (SELECT 1 FROM user_scrape_site us WHERE us.scrape_site_id = scrape_sites.id)").Delete(&domain.ScrapeSite{}).Error; err != nil {
			return err
		}
	}
	if len(judgeIDs) > 0 {
		return tx.Where("id IN ?", judgeIDs).Where("NOT EXISTS (SELECT 1 FROM user_judges uj WHERE uj.judge_id = judges.id)").Where("NOT EXISTS (SELECT 1 FROM proxy_statistics ps WHERE ps.judge_id = judges.id)").Delete(&domain.Judge{}).Error
	}
	return nil
}
