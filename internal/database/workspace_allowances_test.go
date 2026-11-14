package database

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/checkpace"
	"magpie/internal/config"
	"magpie/internal/domain"
	workspaces "magpie/internal/workspace"

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

	if err := workspaces.UpdateWorkspaceAllowances(context.Background(), quiet.ID, dto.WorkspaceAllowanceUpdateRequest{MonthlyChecks: 77, MonthlyScrapes: 0}); err != nil {
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

// A workspace with many tag-rule routes is counted once per planDeltaTTL. In
// between, the count follows the active route count, route changes do not
// discard it, and a count that fails neither fails the load nor loses it.
func TestLargeTagRuleWorkspaceIsCountedOncePerPeriod(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.WorkspaceUsageFlush{}, &domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	owner := domain.User{Email: "large-tag-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	planDeltas.Delete(workspace.ID)
	t.Cleanup(func() { planDeltas.Delete(workspace.ID) })
	addRoutes := func(first, count int) []domain.Proxy {
		t.Helper()
		batch := make([]domain.Proxy, 0, count)
		for index := first; index < first+count; index++ {
			batch = append(batch, domain.Proxy{IP: fmt.Sprintf("203.0.113.%d", 100+index), Port: 8080, Country: "N/A", EstimatedType: "N/A"})
		}
		routes, err := InsertAndGetProxiesWithWorkspace(batch, workspace.ID)
		if err != nil || len(routes) != count {
			t.Fatalf("create routes: %d %v", len(routes), err)
		}
		return routes
	}
	routes := addRoutes(0, 4)

	// Default: HTTP only. A tag rule adds SOCKS5 to tagged routes: to some of
	// the workspace's routes and to more routes than the exact count covers.
	const tagID = 41
	value := domain.Workspace{ID: workspace.ID, CheckerConfig: &dto.CheckerSettings{
		Defaults: dto.CheckerProfileSettings{Protocols: []string{"http"}, Transport: "tcp", Timeout: 1000},
		Rules:    []dto.TagCheckerRule{{TagID: tagID, Mode: "add", Protocols: []string{"socks5"}}},
	}}
	snapshotTagging := func(tagged ...domain.Proxy) *checkerconfig.Workspace {
		assignments := make([]domain.ProxyTagAssignment, 0, len(tagged)+planDeltaExactRoutes)
		for _, route := range tagged {
			assignments = append(assignments, domain.ProxyTagAssignment{WorkspaceID: workspace.ID, ProxyID: route.ID, ProxyTagID: tagID})
		}
		sort.Slice(assignments, func(i, j int) bool { return assignments[i].ProxyID < assignments[j].ProxyID })
		for index := 0; index < planDeltaExactRoutes; index++ {
			assignments = append(assignments, domain.ProxyTagAssignment{WorkspaceID: workspace.ID, ProxyID: uint64(1_000_000 + index), ProxyTagID: tagID})
		}
		snapshot := checkerconfig.Build(value, assignments, nil, nil)
		if snapshot.Proxies.Len() <= planDeltaExactRoutes {
			t.Fatalf("tag rule plans = %d, want more than %d", snapshot.Proxies.Len(), planDeltaExactRoutes)
		}
		return snapshot
	}
	useCheckerSnapshot(t, workspace.ID, snapshotTagging(routes[0], routes[1]))

	now := time.Now()
	perCycle := func(at time.Time) uint64 {
		t.Helper()
		allowances, err := loadWorkspaceAllowances(db, at)
		if err != nil {
			t.Fatal(err)
		}
		return allowances[0].ChecksPerCycle
	}
	// Four active HTTP routes, two of them with SOCKS5 as well.
	if got := perCycle(now); got != 6 {
		t.Fatalf("checks per cycle = %d, want 4 + 2", got)
	}
	// A third route is tagged. The rules are the same, so the count is not
	// repeated yet.
	useCheckerSnapshot(t, workspace.ID, snapshotTagging(routes[0], routes[1], routes[2]))
	if got := perCycle(now.Add(time.Minute)); got != 6 {
		t.Fatalf("checks per cycle = %d right after a tag assignment, want the previous count's 6", got)
	}
	// An import doubles the active routes. Its lifecycle write keeps the
	// count, which follows the route count: twice the routes, twice the extra.
	addRoutes(4, 4)
	if got := perCycle(now.Add(2 * time.Minute)); got != 12 {
		t.Fatalf("checks per cycle = %d after doubling the routes, want 8 + 4", got)
	}
	// A count that fails keeps the estimate and does not fail the load.
	snapshot, _ := checkerconfig.Lookup(workspace.ID)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	later := now.Add(planDeltaTTL + time.Minute)
	countPlanDelta(db.WithContext(cancelled), workspace.ID, snapshot, 8, later)
	if got := loadPlanDelta(workspace.ID).value(snapshot, 1, 8); got != 4 {
		t.Fatalf("a failed count left an extra of %d, want the estimate 4", got)
	}
	// The failed attempt is not repeated at once, and a load without budget
	// left does not count either.
	if got := perCycle(later.Add(time.Minute)); got != 12 {
		t.Fatalf("checks per cycle = %d right after a failed count, want the estimate 12", got)
	}
	expired := later.Add(planDeltaTTL + time.Minute)
	usage := map[uint]workspaceAllowanceUsage{workspace.ID: {activeRoutes: 8}}
	refreshPlanDeltas(db, []uint{workspace.ID}, usage, expired, time.Now().Add(-time.Second))
	if got := loadPlanDelta(workspace.ID).value(snapshot, 1, 8); got != 4 {
		t.Fatalf("a load without budget counted: extra = %d, want the estimate 4", got)
	}
	// The next period's count is exact again: eight routes, three with SOCKS5.
	if got := perCycle(expired); got != 11 {
		t.Fatalf("checks per cycle = %d after the period, want 8 + 3", got)
	}
}

// taggedSnapshot is a checker snapshot whose default is HTTP and whose tag
// rule adds SOCKS5 to the given number of routes.
func taggedSnapshot(workspaceID uint, routes int) *checkerconfig.Workspace {
	const tagID = 41
	assignments := make([]domain.ProxyTagAssignment, routes)
	for index := range assignments {
		assignments[index] = domain.ProxyTagAssignment{WorkspaceID: workspaceID, ProxyID: uint64(index + 1), ProxyTagID: tagID}
	}
	return checkerconfig.Build(domain.Workspace{ID: workspaceID, CheckerConfig: &dto.CheckerSettings{
		Defaults: dto.CheckerProfileSettings{Protocols: []string{"http"}, Transport: "tcp", Timeout: 1000},
		Rules:    []dto.TagCheckerRule{{TagID: tagID, Mode: "add", Protocols: []string{"socks5"}}},
	}}, assignments, nil, nil)
}

// The load's budget covers every count, also the cheap exact ones: without it
// many small workspaces could use the whole refresh. A workspace that was not
// counted is estimated from its checker snapshot.
func TestTagRuleCountsStopWhenTheLoadBudgetIsUsedUp(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	const workspaceID = 998877
	snapshot := taggedSnapshot(workspaceID, 3)
	planDeltas.Delete(uint(workspaceID))
	t.Cleanup(func() { planDeltas.Delete(uint(workspaceID)) })
	useCheckerSnapshot(t, workspaceID, snapshot)
	queries := 0
	if err := db.Callback().Query().Before("gorm:query").Register("test:count-queries", func(*gorm.DB) { queries++ }); err != nil {
		t.Fatal(err)
	}
	usage := map[uint]workspaceAllowanceUsage{workspaceID: {activeRoutes: 10}}
	refreshPlanDeltas(db, []uint{workspaceID}, usage, time.Now(), time.Now().Add(-time.Second))
	if queries != 0 {
		t.Fatalf("%d count queries started after the load's budget had ended", queries)
	}
	// Ten active HTTP routes; the three tagged ones are taken to be active.
	if got := protocolChecksPerCycle(workspaceID, 10); got != 13 {
		t.Fatalf("checks per cycle without a count = %d, want 10 + 3", got)
	}
	// With more tagged routes than active ones, the active routes are taken
	// to be tagged in the same proportion: all of them.
	if got := protocolChecksPerCycle(workspaceID, 2); got != 4 {
		t.Fatalf("checks per cycle without a count = %d for two active routes, want 2 + 2", got)
	}
}

// Counts that do not fit into one load must not always leave out the same
// workspaces. The one that has waited longest is counted first, so every
// workspace gets its turn however many there are.
func TestTagRuleCountsTakeTurnsAcrossLoads(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	const workspaces = 20
	snapshot := taggedSnapshot(1, planDeltaExactRoutes+1)
	ids := make([]uint, 0, workspaces)
	usage := map[uint]workspaceAllowanceUsage{}
	for id := uint(1); id <= workspaces; id++ {
		ids = append(ids, id)
		usage[id] = workspaceAllowanceUsage{activeRoutes: 10_000}
		planDeltas.Delete(id)
	}
	useCheckerSnapshotFor(t, snapshot, ids...)
	t.Cleanup(func() {
		for _, id := range ids {
			planDeltas.Delete(id)
		}
	})
	// Every count query takes longer than the budget of a load, so each load
	// has time for one workspace.
	if err := db.Callback().Query().After("gorm:query").Register("test:slow-count", func(*gorm.DB) { time.Sleep(time.Millisecond) }); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	counted := func() int {
		total := 0
		for _, id := range ids {
			if entry := loadPlanDelta(id); entry.known && !entry.computedAt.IsZero() {
				total++
			}
		}
		return total
	}
	// One load per minute. The counts are repeated every ten minutes, and
	// only one fits into a load: the same ten workspaces must not take every
	// turn.
	for load := 0; load < workspaces; load++ {
		refreshPlanDeltas(db, ids, usage, start.Add(time.Duration(load)*time.Minute), time.Now().Add(time.Millisecond))
		if got := counted(); got != load+1 {
			t.Fatalf("after %d loads %d workspaces were counted", load+1, got)
		}
	}
	// Afterwards the workspace counted first is the one that waited longest.
	refreshPlanDeltas(db, ids, usage, start.Add(workspaces*time.Minute), time.Now().Add(time.Millisecond))
	first, second := loadPlanDelta(1).computedAt, loadPlanDelta(2).computedAt
	if !first.Equal(start.Add(workspaces*time.Minute)) || !second.Equal(start.Add(time.Minute)) {
		t.Fatalf("the second round counted workspace 1 at %s and workspace 2 at %s", first.Sub(start), second.Sub(start))
	}
}

// A lifecycle write asks for a new count. It must not move the workspace to
// the front of the counts that are due, or a workspace that changes before
// every load would take every turn, and it must not shorten the wait after a
// count that failed.
func TestRouteChangesKeepAWorkspacesTurnAndItsWaitAfterAFailedCount(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	ids := []uint{99101, 99102}
	snapshot := taggedSnapshot(ids[0], 1)
	useCheckerSnapshotFor(t, snapshot, ids...)
	usage := map[uint]workspaceAllowanceUsage{ids[0]: {activeRoutes: 10}, ids[1]: {activeRoutes: 10}}
	for _, id := range ids {
		planDeltas.Delete(id)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			planDeltas.Delete(id)
		}
	})
	// Every load has time for one count.
	if err := db.Callback().Query().After("gorm:query").Register("test:slow-count", func(*gorm.DB) { time.Sleep(3 * time.Millisecond) }); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for load := 0; load < 2; load++ {
		invalidatePlanDeltas(ids[0])
		refreshPlanDeltas(db, ids, usage, now.Add(time.Duration(load)*time.Minute), time.Now().Add(time.Millisecond))
	}
	if !loadPlanDelta(ids[1]).known {
		t.Fatal("a workspace whose routes change before every load took the other workspace's turn")
	}
	// The changed workspace is counted again when its turn comes.
	invalidatePlanDeltas(ids[0])
	refreshPlanDeltas(db, ids, usage, now.Add(2*time.Minute), time.Now().Add(time.Millisecond))
	if entry := loadPlanDelta(ids[0]); entry.changed || !entry.computedAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("the changed workspace was not counted again: %+v", entry)
	}

	failed := planDeltaEntry{snapshot: snapshot, signature: snapshot.InputSignature, activeRoutes: 10, delta: 1, computedAt: now, known: true, failed: true}
	planDeltas.Store(ids[0], failed)
	invalidatePlanDeltas(ids[0])
	if loadPlanDelta(ids[0]).due(snapshot, 11, now.Add(time.Minute)) {
		t.Fatal("a lifecycle write repeats a failed count before its ten minutes are over")
	}
	if !loadPlanDelta(ids[0]).due(snapshot, 11, now.Add(planDeltaTTL)) {
		t.Fatal("a failed count is not repeated after its ten minutes")
	}
}

// Without a count, rules that remove protocols must not lower the planned
// checks: the tagged routes may all be paused. Planning too few checks per
// cycle makes an automatic cadence too fast, down to the minimum interval.
func TestUncountedTagRulesNeverPlanFewerChecksThanTheDefaults(t *testing.T) {
	const workspaceID, tagID = 99103, 41
	value := domain.Workspace{ID: workspaceID, CheckerConfig: &dto.CheckerSettings{
		Defaults: dto.CheckerProfileSettings{Protocols: []string{"http"}, Transport: "tcp", Timeout: 1000},
		Rules:    []dto.TagCheckerRule{{TagID: tagID, Mode: "remove", Protocols: []string{"http"}}},
	}}
	// A hundred tagged routes, which may be paused, beside a hundred active
	// routes without a tag.
	assignments := make([]domain.ProxyTagAssignment, 100)
	for index := range assignments {
		assignments[index] = domain.ProxyTagAssignment{WorkspaceID: workspaceID, ProxyID: uint64(1000 + index), ProxyTagID: tagID}
	}
	useCheckerSnapshot(t, workspaceID, checkerconfig.Build(value, assignments, nil, nil))
	planDeltas.Delete(uint(workspaceID))
	t.Cleanup(func() { planDeltas.Delete(uint(workspaceID)) })
	if got := protocolChecksPerCycle(workspaceID, 100); got != 100 {
		t.Fatalf("checks per cycle without a count = %d, want the 100 of the defaults", got)
	}
}

// A count says how many tagged routes were active under the rules and
// defaults it was made for. After they change it must not be reused, also when
// the new count cannot run or fails.
func TestTagRuleCountMadeUnderOtherRulesIsNotReused(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	const workspaceID, routes = 998877, 10_000
	snapshot := taggedSnapshot(workspaceID, routes)
	// The previous default checked four protocols and its rule left one.
	planDeltas.Store(uint(workspaceID), planDeltaEntry{
		signature: "previous-defaults-and-rules", activeRoutes: routes, delta: -30_000,
		computedAt: time.Now(), known: true, throttled: true,
	})
	t.Cleanup(func() { planDeltas.Delete(uint(workspaceID)) })
	useCheckerSnapshot(t, workspaceID, snapshot)
	usage := map[uint]workspaceAllowanceUsage{workspaceID: {activeRoutes: routes}}

	// No budget for the count: HTTP on every route, SOCKS5 on the tagged ones.
	refreshPlanDeltas(db, []uint{workspaceID}, usage, time.Now(), time.Now().Add(-time.Second))
	if got := protocolChecksPerCycle(workspaceID, routes); got != 2*routes {
		t.Fatalf("checks per cycle = %d under the new rules without a count, want %d", got, 2*routes)
	}
	// A count that fails keeps that estimate, and stores it for these rules.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	refreshPlanDeltas(db.WithContext(cancelled), []uint{workspaceID}, usage, time.Now(), time.Now().Add(time.Minute))
	entry := loadPlanDelta(workspaceID)
	if !entry.failed || entry.signature != snapshot.InputSignature || entry.delta != routes {
		t.Fatalf("after a failed count the entry is %+v, want the estimate %d for the current rules", entry, routes)
	}
	if got := protocolChecksPerCycle(workspaceID, routes); got != 2*routes {
		t.Fatalf("checks per cycle = %d after a failed count, want %d", got, 2*routes)
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
	useCheckerSnapshotFor(t, snapshot, workspaceID)
}

// useCheckerSnapshotFor gives every listed workspace the same checker snapshot.
func useCheckerSnapshotFor(t *testing.T, snapshot *checkerconfig.Workspace, workspaceIDs ...uint) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := checkerconfig.Initialize(ctx, nil,
		func(context.Context, uint) (*checkerconfig.Workspace, error) { return snapshot, nil },
		func(context.Context) ([]uint, error) { return workspaceIDs, nil }); err != nil {
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
	allowances[0].ChecksPerCycle = 100
	snapshot := checkpace.Compute(time.Now(), allowances)
	var subscription domain.WorkspaceSubscription
	if err := db.Where("workspace_id = ?", workspace.ID).First(&subscription).Error; err != nil {
		t.Fatal(err)
	}
	warnings := CadenceWarnings(snapshot, workspace.ID, subscription, request)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "more than the allowance of 10000") {
		t.Fatalf("warnings = %q", warnings)
	}
	if got := CadenceWarnings(snapshot, workspace.ID, subscription, dto.WorkspaceCadenceUpdateRequest{}); len(got) != 0 {
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

	if err := workspaces.UpdateWorkspaceAllowances(ctx, workspace.ID, dto.WorkspaceAllowanceUpdateRequest{UnlimitedChecks: true}); err != nil {
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
	allowances[0].ChecksPerCycle = 1_000_000
	snapshot := checkpace.Compute(time.Now(), allowances)
	var subscription domain.WorkspaceSubscription
	if err := db.Where("workspace_id = ?", workspace.ID).First(&subscription).Error; err != nil {
		t.Fatal(err)
	}
	if warnings := CadenceWarnings(snapshot, workspace.ID, subscription, request); len(warnings) != 0 {
		t.Fatalf("an unlimited workspace was warned about its allowance: %q", warnings)
	}
	stored := domain.Workspace{ID: workspace.ID, CheckIntervalSeconds: 60}
	if got := workspaces.WorkspaceAllowancesDTO(snapshot, stored, subscription); !got.UnlimitedChecks || got.NeedsCheckInterval || got.InheritsChecks || got.ChecksRunOutAt != nil {
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
	if got := workspaces.WorkspaceAllowancesDTO(snapshot, stored, subscription); !got.UnlimitedScrapes || !got.NeedsScrapeInterval {
		t.Fatalf("inherited unlimited scrapes = %+v", got)
	}
	// An explicit monthly allowance overrides the unlimited default.
	if err := workspaces.UpdateWorkspaceAllowances(ctx, workspace.ID, dto.WorkspaceAllowanceUpdateRequest{UnlimitedChecks: true, MonthlyScrapes: 500}); err != nil {
		t.Fatal(err)
	}
	if err := UpdateWorkspaceCadence(ctx, workspace.ID, request); err != nil {
		t.Fatalf("limited scrapes may stay automatic: %v", err)
	}
}
