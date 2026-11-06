package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkpace"
	"magpie/internal/config"
	"magpie/internal/domain"
)

func TestWorkspaceAllowancesMeterChecksScrapesAndInheritDefaults(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatalf("migrate usage tables: %v", err)
	}
	t.Chdir(t.TempDir())
	original := config.GetConfig()
	t.Cleanup(func() {
		if err := config.SetConfig(original); err != nil {
			t.Error(err)
		}
	})
	cfg := original
	cfg.Checker.MonthlyChecksPerWorkspace = 1_000
	cfg.Scraper.MonthlyScrapesPerWorkspace = 50
	if err := config.SetConfig(cfg); err != nil {
		t.Fatalf("set instance allowances: %v", err)
	}

	owner := domain.User{Email: "allowance-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	other := domain.User{Email: "allowance-other@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	quiet := createTestWorkspaceForUser(t, db, other)

	inserted, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{
		{IP: "192.0.2.10", Port: 8080, Country: "N/A", EstimatedType: "N/A"},
		{IP: "192.0.2.11", Port: 8080, Country: "N/A", EstimatedType: "N/A"},
	}, workspace.ID)
	if err != nil || len(inserted) != 2 {
		t.Fatalf("create managed routes: count=%d error=%v", len(inserted), err)
	}
	if _, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{{IP: "192.0.2.12", Port: 8080, Country: "N/A", EstimatedType: "N/A"}}, quiet.ID); err != nil {
		t.Fatal(err)
	}
	// The quiet workspace has no usage row, so its active count comes from a
	// direct count of its managed proxies.
	if err := db.Where("workspace_id = ?", quiet.ID).Delete(&domain.WorkspaceUsagePeriod{}).Error; err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	statistics := []domain.ProxyStatistic{
		{ProxyID: inserted[0].ID, Attempt: 2, WorkspaceIDs: []uint{workspace.ID}, CreatedAt: now},
		{ProxyID: inserted[1].ID, Attempt: 0, WorkspaceIDs: []uint{workspace.ID, workspace.ID}, CreatedAt: now},
	}
	if err := recordWorkspaceCheckUsage(db, statistics); err != nil {
		t.Fatalf("record check usage: %v", err)
	}
	if err := recordWorkspaceScrapeUsage(db, []uint{workspace.ID, workspace.ID}, now); err != nil {
		t.Fatalf("record scrape usage: %v", err)
	}
	site := domain.ScrapeSite{URL: "https://lists.example.test/proxies.txt"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
		t.Fatal(err)
	}

	periodStart, _ := workspaceUsageMonth(now)
	var usage domain.WorkspaceUsagePeriod
	if err := db.Where("workspace_id = ? AND period_start = ?", workspace.ID, periodStart).First(&usage).Error; err != nil {
		t.Fatal(err)
	}
	// Retries count as attempts, not as extra checks.
	if usage.Checks != 2 || usage.CheckAttempts != 4 || usage.Scrapes != 1 {
		t.Fatalf("usage = checks %d, attempts %d, scrapes %d; want 2, 4, 1", usage.Checks, usage.CheckAttempts, usage.Scrapes)
	}

	if err := UpdateWorkspaceAllowances(context.Background(), quiet.ID, dto.WorkspaceAllowanceUpdateRequest{MonthlyChecks: 77, MonthlyScrapes: 0}); err != nil {
		t.Fatalf("set workspace override: %v", err)
	}

	allowances, err := loadWorkspaceAllowances(db, now)
	if err != nil {
		t.Fatalf("load allowances: %v", err)
	}
	byWorkspace := map[uint]struct{ checks, used, perCycle, scrapes, usedScrapes, sources uint64 }{}
	for _, allowance := range allowances {
		byWorkspace[allowance.WorkspaceID] = struct{ checks, used, perCycle, scrapes, usedScrapes, sources uint64 }{
			allowance.MonthlyChecks, allowance.UsedChecks, allowance.ChecksPerCycle,
			allowance.MonthlyScrapes, allowance.UsedScrapes, allowance.ScrapesPerCycle,
		}
	}
	got := byWorkspace[workspace.ID]
	if got.checks != 1_000 || got.used != 2 || got.perCycle != 2 || got.scrapes != 50 || got.usedScrapes != 1 || got.sources != 1 {
		t.Fatalf("inheriting workspace allowance = %+v", got)
	}
	got = byWorkspace[quiet.ID]
	if got.checks != 77 || got.used != 0 || got.perCycle != 1 || got.scrapes != 50 {
		t.Fatalf("overriding workspace allowance = %+v", got)
	}
}

func TestLegacyCadenceAllowancesKeepBusiestWorkspaceCadence(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	owner := domain.User{Email: "legacy-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	proxies := make([]domain.Proxy, 0, 40)
	for i := 0; i < 40; i++ {
		proxies = append(proxies, domain.Proxy{IP: fmt.Sprintf("198.51.100.%d", i+1), Port: 8080, Country: "N/A", EstimatedType: "N/A"})
	}
	if _, err := InsertAndGetProxiesWithWorkspace(proxies, workspace.ID); err != nil {
		t.Fatal(err)
	}

	checks, scrapes, err := LegacyCadenceAllowances(context.Background(), time.Minute, 5*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// 40 routes every minute for 31 days exceeds the shipped default.
	if want := uint64(40 * 31 * 24 * 60); checks != want {
		t.Fatalf("converted checks = %d, want %d", checks, want)
	}
	// No subscribed sources: the default still applies.
	if scrapes != config.DefaultMonthlyScrapesPerWorkspace {
		t.Fatalf("converted scrapes = %d, want default", scrapes)
	}

	slow, _, err := LegacyCadenceAllowances(context.Background(), 24*time.Hour, 0)
	if err != nil {
		t.Fatal(err)
	}
	if slow != config.DefaultMonthlyChecksPerWorkspace {
		t.Fatalf("a slow legacy timer must not lower the default: %d", slow)
	}
}

func TestWorkspaceCadenceValidationLoadingAndWarnings(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	owner := domain.User{Email: "cadence-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	if err := db.Model(&domain.WorkspaceSubscription{}).Where("workspace_id = ?", workspace.ID).
		Updates(map[string]any{"minimum_check_interval_seconds": 300, "monthly_checks": 10_000}).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for _, invalid := range []dto.WorkspaceCadenceUpdateRequest{
		{CheckIntervalSeconds: 120},
		{ScrapeIntervalSeconds: 60},
		{CheckIntervalSeconds: 40 * 24 * 3600},
	} {
		if err := UpdateWorkspaceCadence(ctx, workspace.ID, invalid); err == nil {
			t.Fatalf("invalid cadence %+v was accepted", invalid)
		}
	}
	request := dto.WorkspaceCadenceUpdateRequest{CheckIntervalSeconds: 600, ScrapeIntervalSeconds: 3600}
	if err := UpdateWorkspaceCadence(ctx, workspace.ID, request); err != nil {
		t.Fatalf("valid cadence rejected: %v", err)
	}

	allowances, err := loadWorkspaceAllowances(db, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(allowances) != 1 || allowances[0].RequestedCheckInterval != 10*time.Minute || allowances[0].RequestedScrapeInterval != time.Hour {
		t.Fatalf("requested intervals not loaded: %+v", allowances)
	}

	// 100 routes every 10 minutes needs about 432,000 checks a month.
	previous := checkpace.Current()
	t.Cleanup(func() { checkpace.Publish(previous) })
	allowances[0].ChecksPerCycle = 100
	checkpace.Publish(checkpace.Compute(time.Now(), allowances))
	var subscription domain.WorkspaceSubscription
	if err := db.Where("workspace_id = ?", workspace.ID).First(&subscription).Error; err != nil {
		t.Fatal(err)
	}
	warnings := CadenceWarnings(workspace.ID, subscription, request)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "more than the allowance of 10000") {
		t.Fatalf("warnings = %q", warnings)
	}
	if got := CadenceWarnings(workspace.ID, subscription, dto.WorkspaceCadenceUpdateRequest{}); len(got) != 0 {
		t.Fatalf("automatic cadence produced warnings: %q", got)
	}
}

func TestUnlimitedWorkspaceRequiresFixedIntervalAndInheritsInstanceDefault(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	original := config.GetConfig()
	t.Cleanup(func() {
		if err := config.SetConfig(original); err != nil {
			t.Error(err)
		}
	})
	owner := domain.User{Email: "unlimited-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	ctx := context.Background()

	if err := UpdateWorkspaceAllowances(ctx, workspace.ID, dto.WorkspaceAllowanceUpdateRequest{UnlimitedChecks: true}); err != nil {
		t.Fatalf("set unlimited checks: %v", err)
	}
	// Automatic spreading needs a limit, so a check interval is required. The
	// scrape allowance is still limited and may stay automatic.
	if err := UpdateWorkspaceCadence(ctx, workspace.ID, dto.WorkspaceCadenceUpdateRequest{ScrapeIntervalSeconds: 3600}); !errors.Is(err, ErrUnlimitedNeedsInterval) {
		t.Fatalf("automatic checks on an unlimited workspace: err = %v", err)
	}
	request := dto.WorkspaceCadenceUpdateRequest{CheckIntervalSeconds: 60}
	if err := UpdateWorkspaceCadence(ctx, workspace.ID, request); err != nil {
		t.Fatalf("fixed interval rejected: %v", err)
	}

	allowances, err := loadWorkspaceAllowances(db, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(allowances) != 1 || !allowances[0].UnlimitedChecks || allowances[0].UnlimitedScrapes {
		t.Fatalf("unlimited flags not loaded: %+v", allowances)
	}
	previous := checkpace.Current()
	t.Cleanup(func() { checkpace.Publish(previous) })
	allowances[0].ChecksPerCycle = 1_000_000
	checkpace.Publish(checkpace.Compute(time.Now(), allowances))
	var subscription domain.WorkspaceSubscription
	if err := db.Where("workspace_id = ?", workspace.ID).First(&subscription).Error; err != nil {
		t.Fatal(err)
	}
	if warnings := CadenceWarnings(workspace.ID, subscription, request); len(warnings) != 0 {
		t.Fatalf("an unlimited workspace was warned about its allowance: %q", warnings)
	}
	stored := domain.Workspace{ID: workspace.ID, CheckIntervalSeconds: 60}
	if got := workspaceAllowancesDTO(stored, subscription); !got.UnlimitedChecks || got.NeedsCheckInterval || got.InheritsChecks || got.ChecksRunOutAt != nil {
		t.Fatalf("allowance view = %+v", got)
	}

	// An unlimited instance default reaches workspaces that inherit it.
	cfg := original
	cfg.Scraper.UnlimitedMonthlyScrapes = true
	if err := config.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := UpdateWorkspaceCadence(ctx, workspace.ID, request); !errors.Is(err, ErrUnlimitedNeedsInterval) {
		t.Fatalf("automatic scrapes under an unlimited default: err = %v", err)
	}
	if got := workspaceAllowancesDTO(stored, subscription); !got.UnlimitedScrapes || !got.NeedsScrapeInterval {
		t.Fatalf("inherited unlimited scrapes = %+v", got)
	}
	// An explicit monthly allowance overrides the unlimited default.
	if err := UpdateWorkspaceAllowances(ctx, workspace.ID, dto.WorkspaceAllowanceUpdateRequest{UnlimitedChecks: true, MonthlyScrapes: 500}); err != nil {
		t.Fatal(err)
	}
	if err := UpdateWorkspaceCadence(ctx, workspace.ID, request); err != nil {
		t.Fatalf("limited scrapes may stay automatic: %v", err)
	}
}
