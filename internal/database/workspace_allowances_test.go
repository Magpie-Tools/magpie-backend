package database

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/checkpace"
	"magpie/internal/config"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestWorkspaceAllowancesMeterChecksScrapesAndInheritDefaults(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceUsageFlush{}, &domain.WorkspaceScrapeSite{}); err != nil {
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
	// Allowance usage comes from the checker's own meter, not from statistics.
	if err := flushWorkspaceCheckUsage(db, testUsageBatch(checkpace.CheckUsage{WorkspaceID: workspace.ID, PeriodStart: now, Attempts: 4, ProtocolChecks: 2})); err != nil {
		t.Fatalf("flush metered check usage: %v", err)
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
	// Every request, retries included, is a charged check.
	if usage.Checks != 4 || usage.ProtocolChecks != 2 || usage.CheckAttempts != 4 || usage.Scrapes != 1 {
		t.Fatalf("usage = checks %d, protocol checks %d, attempts %d, scrapes %d; want 4, 2, 4, 1",
			usage.Checks, usage.ProtocolChecks, usage.CheckAttempts, usage.Scrapes)
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
	if got.checks != 1_000 || got.used != 4 || got.perCycle != 2 || got.scrapes != 50 || got.usedScrapes != 1 || got.sources != 1 {
		t.Fatalf("inheriting workspace allowance = %+v", got)
	}
	got = byWorkspace[quiet.ID]
	if got.checks != 77 || got.used != 0 || got.perCycle != 1 || got.scrapes != 50 {
		t.Fatalf("overriding workspace allowance = %+v", got)
	}
}

func TestPreserveLegacyCadenceKeepsEveryWorkspaceAtItsOldTimer(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	owner := domain.User{Email: "legacy-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	plain := createTestWorkspaceForUser(t, db, owner)
	other := domain.User{Email: "legacy-other@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	slowPlan := createTestWorkspaceForUser(t, db, other)
	if err := db.Model(&domain.WorkspaceSubscription{}).Where("workspace_id = ?", slowPlan.ID).
		Update("minimum_check_interval_seconds", 6*3600).Error; err != nil {
		t.Fatal(err)
	}
	third := domain.User{Email: "legacy-chosen@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&third).Error; err != nil {
		t.Fatal(err)
	}
	chosen := createTestWorkspaceForUser(t, db, third)
	if err := db.Model(&domain.Workspace{}).Where("id = ?", chosen.ID).Update("check_interval_seconds", 600).Error; err != nil {
		t.Fatal(err)
	}

	checks, scrapes, err := preserveLegacyCadence(db, 5*time.Hour, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if checks != 2 || scrapes != 3 {
		t.Fatalf("converted %d check and %d scrape intervals, want 2 and 3", checks, scrapes)
	}
	interval := func(id uint) (uint32, uint32) {
		var workspace domain.Workspace
		if err := db.Select("check_interval_seconds", "scrape_interval_seconds").First(&workspace, id).Error; err != nil {
			t.Fatal(err)
		}
		return workspace.CheckIntervalSeconds, workspace.ScrapeIntervalSeconds
	}
	// The old five-hour timer stays five hours: no workspace starts checking
	// every minute after the upgrade.
	if check, scrape := interval(plain.ID); check != 5*3600 || scrape != 15*60 {
		t.Fatalf("plain workspace intervals = %d / %d", check, scrape)
	}
	if check, _ := interval(slowPlan.ID); check != 6*3600 {
		t.Fatalf("plan minimum not applied: %d", check)
	}
	if check, _ := interval(chosen.ID); check != 600 {
		t.Fatalf("an interval chosen before the upgrade was replaced: %d", check)
	}
}

func TestChecksPerCycleCountsOnlyActiveRoutesAndObservedRetries(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceUsageFlush{}, &domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	owner := domain.User{Email: "tag-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	routes, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{
		{IP: "203.0.113.10", Port: 8080, Country: "N/A", EstimatedType: "N/A"},
		{IP: "203.0.113.11", Port: 8080, Country: "N/A", EstimatedType: "N/A"},
	}, workspace.ID)
	if err != nil || len(routes) != 2 {
		t.Fatalf("create routes: %d %v", len(routes), err)
	}
	paused := routes[1]
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND proxy_id = ?", workspace.ID, paused.ID).
		Update("state", domain.ManagedProxyStatePaused).Error; err != nil {
		t.Fatal(err)
	}
	if err := refreshWorkspaceUsageActiveRoutes(db, workspace.ID); err != nil {
		t.Fatal(err)
	}

	// Default: HTTP only. A tag rule adds SOCKS5 to the paused route alone.
	const tagID = 41
	value := domain.Workspace{ID: workspace.ID, CheckerConfig: &dto.CheckerSettings{
		Defaults: dto.CheckerProfileSettings{Protocols: []string{"http"}, Transport: "tcp", Timeout: 1000},
		Rules:    []dto.TagCheckerRule{{TagID: tagID, Mode: "add", Protocols: []string{"socks5"}}},
	}}
	snapshot := checkerconfig.Build(value, []domain.ProxyTagAssignment{{WorkspaceID: workspace.ID, ProxyID: paused.ID, ProxyTagID: tagID}}, nil, nil)
	if snapshot.Proxies.Len() != 1 {
		t.Fatalf("tag rule plans = %d, want 1", snapshot.Proxies.Len())
	}
	useCheckerSnapshot(t, workspace.ID, snapshot)

	now := time.Now()
	perCycle := func() uint64 {
		allowances, err := loadWorkspaceAllowances(db, now)
		if err != nil {
			t.Fatal(err)
		}
		return allowances[0].ChecksPerCycle
	}
	// One active HTTP route: one check per cycle. The paused route's extra
	// protocol does not count.
	if got := perCycle(); got != 1 {
		t.Fatalf("checks per cycle = %d, want 1", got)
	}

	// Activating the tagged route invalidates the cached count.
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND proxy_id = ?", workspace.ID, paused.ID).
		Update("state", domain.ManagedProxyStateActive).Error; err != nil {
		t.Fatal(err)
	}
	if err := refreshWorkspaceUsageActiveRoutes(db, workspace.ID); err != nil {
		t.Fatal(err)
	}
	if got := perCycle(); got != 3 {
		t.Fatalf("checks per cycle after activation = %d, want 3 (HTTP + HTTP and SOCKS5)", got)
	}

	// Retries observed this month raise the estimate: 300 requests for 200
	// protocol checks is 1.5 requests per check.
	if err := flushWorkspaceCheckUsage(db, testUsageBatch(checkpace.CheckUsage{WorkspaceID: workspace.ID, PeriodStart: now, Attempts: 300, ProtocolChecks: 200})); err != nil {
		t.Fatal(err)
	}
	if got := perCycle(); got != 5 {
		t.Fatalf("checks per cycle with retries = %d, want ceil(3 × 1.5) = 5", got)
	}
}

var testUsageSequence uint64

// testUsageBatch numbers a flush batch the way one checker process would.
func testUsageBatch(usage ...checkpace.CheckUsage) checkpace.UsageBatch {
	testUsageSequence++
	return checkpace.UsageBatch{Instance: "test-instance", Sequence: testUsageSequence, Usage: usage}
}

func TestReplayedUsageBatchIsAppliedOnceInItsOwnMonth(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) { testReplayedUsageBatch(t, setupRotatingProxyTestDB(t)) })
	t.Run("postgres", func(t *testing.T) { testReplayedUsageBatch(t, setupProxyIngestionPostgresTest(t)) })
}

func testReplayedUsageBatch(t *testing.T, db *gorm.DB) {
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceUsageFlush{}); err != nil {
		t.Fatal(err)
	}
	workspace := domain.Workspace{ID: 42}
	thisMonth, _ := workspaceUsageMonth(time.Now())
	lastMonth := thisMonth.AddDate(0, -1, 0)
	batch := checkpace.UsageBatch{Instance: "replay-instance", Sequence: 1, Usage: []checkpace.CheckUsage{
		{WorkspaceID: workspace.ID, PeriodStart: lastMonth, Attempts: 2, ProtocolChecks: 1},
		{WorkspaceID: workspace.ID, PeriodStart: thisMonth, Attempts: 3, ProtocolChecks: 1},
	}}
	// The first commit's acknowledgement was lost, so the checker resends the
	// same batch.
	for range 2 {
		if err := flushWorkspaceCheckUsage(db, batch); err != nil {
			t.Fatal(err)
		}
	}
	// Another process's batch with the same number is separate usage.
	if err := flushWorkspaceCheckUsage(db, checkpace.UsageBatch{Instance: "other-instance", Sequence: 1, Usage: []checkpace.CheckUsage{
		{WorkspaceID: workspace.ID, PeriodStart: thisMonth, Attempts: 1, ProtocolChecks: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	checks := func(period time.Time) uint64 {
		var usage domain.WorkspaceUsagePeriod
		if err := db.Where("workspace_id = ? AND period_start = ?", workspace.ID, period).First(&usage).Error; err != nil {
			t.Fatal(err)
		}
		return usage.Checks
	}
	if previous, current := checks(lastMonth), checks(thisMonth); previous != 2 || current != 4 {
		t.Fatalf("checks = %d last month, %d this month; want 2 and 4", previous, current)
	}
	// A batch without a sequence could not be told apart from a replay.
	batch.Sequence = 0
	if err := flushWorkspaceCheckUsage(db, batch); err == nil {
		t.Fatal("a batch without a sequence was accepted")
	}
}

func TestResendAfterItsReplayMarkWasPrunedIsNotApplied(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) { testPrunedReplayMark(t, setupRotatingProxyTestDB(t), "pruned-sqlite") })
	t.Run("postgres", func(t *testing.T) { testPrunedReplayMark(t, setupProxyIngestionPostgresTest(t), "pruned-postgres") })
}

func testPrunedReplayMark(t *testing.T, db *gorm.DB, instance string) {
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceUsageFlush{}); err != nil {
		t.Fatal(err)
	}
	thisMonth, _ := workspaceUsageMonth(time.Now())
	usage := []checkpace.CheckUsage{{WorkspaceID: 43, PeriodStart: thisMonth, Attempts: 5, ProtocolChecks: 1}}
	checks := func() uint64 {
		var period domain.WorkspaceUsagePeriod
		db.Where("workspace_id = 43").Limit(1).Find(&period)
		return period.Checks
	}
	prune := func() {
		if err := db.Where("instance = ?", instance).Delete(&domain.WorkspaceUsageFlush{}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// An empty keepalive batch registers and refreshes the mark only.
	if err := flushWorkspaceCheckUsage(db, checkpace.UsageBatch{Instance: instance, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	var mark domain.WorkspaceUsageFlush
	if err := db.First(&mark, "instance = ?", instance).Error; err != nil || mark.Sequence != 1 || checks() != 0 {
		t.Fatalf("keepalive left mark %+v and %d checks: %v", mark, checks(), err)
	}
	// A first send after its mark was pruned cannot have been applied.
	prune()
	if err := flushWorkspaceCheckUsage(db, checkpace.UsageBatch{Instance: instance, Sequence: 2, Usage: usage}); err != nil {
		t.Fatal(err)
	}
	// A resend after its mark was pruned may have been applied: refuse it.
	prune()
	err := flushWorkspaceCheckUsage(db, checkpace.UsageBatch{Instance: instance, Sequence: 2, Usage: usage, Resend: true})
	if !errors.Is(err, checkpace.ErrBatchOutcomeUnknown) || checks() != 5 {
		t.Fatalf("pruned resend: err = %v, checks = %d; want ErrBatchOutcomeUnknown and 5", err, checks())
	}
}

// useCheckerSnapshot publishes a checker snapshot for one workspace until the
// test ends.
func useCheckerSnapshot(t *testing.T, workspaceID uint, snapshot *checkerconfig.Workspace) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := checkerconfig.Initialize(ctx, nil,
		func(context.Context, uint) (*checkerconfig.Workspace, error) { return snapshot, nil },
		func(context.Context) ([]uint, error) { return []uint{workspaceID}, nil }); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		stopped, stop := context.WithCancel(context.Background())
		stop()
		_ = checkerconfig.Initialize(stopped, nil, nil, func(context.Context) ([]uint, error) { return nil, nil })
	})
}

func TestWorkspaceCadenceValidationLoadingAndWarnings(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceUsageFlush{}, &domain.WorkspaceScrapeSite{}); err != nil {
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
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceUsageFlush{}, &domain.WorkspaceScrapeSite{}); err != nil {
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
