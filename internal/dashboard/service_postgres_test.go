package dashboard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	"magpie/internal/platform/schema"
	"magpie/internal/readmodel"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// snapshotFixture opens a PostgreSQL schema with the evidence seed, the read
// model projected for every workspace and the lease sequence.
func snapshotFixture(t *testing.T) (*gorm.DB, []domain.Workspace) {
	t.Helper()
	db := setupProxyIngestionPostgresTest(t)
	workspaces := seedCurrentLatestReadTest(t, db)
	for _, step := range []func(*gorm.DB) error{schema.EnsureReadModelSchema, schema.EnsureDashboardSnapshotSchema} {
		if err := step(db); err != nil {
			t.Fatal(err)
		}
	}
	for _, workspace := range workspaces {
		if err := readmodel.RefreshWorkspaceProxyFilterIndexes(db, "WHERE up.workspace_id = ?", workspace.ID); err != nil {
			t.Fatal(err)
		}
	}
	return db, workspaces
}

type statementLog struct {
	logger.Interface
	mu         sync.Mutex
	statements []string
}

func (l *statementLog) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	l.mu.Lock()
	l.statements = append(l.statements, sql)
	l.mu.Unlock()
}

func (l *statementLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	statements := l.statements
	l.statements = nil
	return statements
}

func newTestService(db *gorm.DB, cfg Config) *Service { return NewService(db, cfg) }

func TestViewBuildsOnceThenServesFromTheSnapshotPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0]
	period, _ := checkpace.UsagePeriod(time.Now())
	if err := db.Create(&[]domain.WorkspaceUsagePeriod{
		{WorkspaceID: owner.ID, PeriodStart: period.AddDate(0, -1, 0), PeriodEnd: period, CheckAttempts: 40},
		{WorkspaceID: owner.ID, PeriodStart: period, PeriodEnd: period.AddDate(0, 1, 0), CheckAttempts: 3, Checks: 7},
	}).Error; err != nil {
		t.Fatal(err)
	}
	// More candidates than the stored 50: alive and dead routes, shared check
	// times (ties broken by route ID) and routes never checked (NULL times).
	for _, statement := range []string{
		`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type) SELECT n,'198.51.100.7','198.51.100.7',9000+n,'FR','' FROM generate_series(100,169) n`,
		`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(100,169) n`,
		`INSERT INTO proxy_latest_statistics (workspace_id,config_key,transport_protocol,proxy_id,protocol_id,alive,statistic_id,response_time,attempt,judge_id,checked_at,created_at,updated_at)
		 SELECT ?,'http-a','tcp',n,1,n%3<>0,n*10+1,40+n%7,0,1,'2026-10-21 13:00:00+00'::timestamptz+(n%5)*interval '1 minute',now(),now()
		 FROM generate_series(100,159) n`,
	} {
		var args []any
		if strings.Contains(statement, "?") {
			args = append(args, owner.ID)
		}
		if err := db.Exec(statement, args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := readmodel.RefreshWorkspaceProxyFilterIndexes(db, "WHERE up.workspace_id = ?", owner.ID); err != nil {
		t.Fatal(err)
	}
	log := &statementLog{Interface: logger.Default.LogMode(logger.Silent)}
	service := newTestService(db.Session(&gorm.Session{Logger: log}), Config{})
	defer service.Stop()

	cold, err := service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cold.Status != StatusFresh || cold.TotalChecks != 47 || cold.ChecksThisMonth != 7 || cold.CollectedAt == nil || cold.ComputedAt == nil {
		t.Fatalf("cold view: %+v", cold)
	}
	if cold.ProxyCount() == nil || *cold.ProxyCount() != 13+70 || cold.AliveCount() == nil || cold.TotalScraped() == nil {
		t.Fatalf("cold view parts: %+v", cold)
	}

	// The snapshot SQL is exactly the existing queries' results.
	direct, err := queryFastestAliveProxies(db, owner.ID, fastestLimit)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1, 2, 3, 100} {
		got := cold.FastestAliveProxies(limit)
		if len(got) != min(limit, len(direct)) {
			t.Fatalf("fastest limit %d: %d rows, want %d", limit, len(got), min(limit, len(direct)))
		}
		for i := range got {
			// Compare instants, not time.Time values: a decoded payload
			// and a scanned row can carry different Location pointers.
			g, d := got[i], direct[i]
			if !g.LatestCheck.Equal(d.LatestCheck) {
				t.Fatalf("fastest limit %d row %d latest check: %v, want %v", limit, i, g.LatestCheck, d.LatestCheck)
			}
			g.LatestCheck, d.LatestCheck = time.Time{}, time.Time{}
			if g != d {
				t.Fatalf("fastest limit %d row %d: %+v, want %+v", limit, i, got[i], direct[i])
			}
		}
	}
	for limit := 1; limit <= recentCandidateLimit; limit++ {
		want, err := queryRecentProxyChecks(db, owner.ID, limit)
		if err != nil {
			t.Fatal(err)
		}
		got := cold.RecentProxyChecks(limit)
		if len(got) != len(want) {
			t.Fatalf("recent limit %d: %d rows, want %d", limit, len(got), len(want))
		}
		for i := range got {
			w := want[i]
			if got[i].ID != w.ID || got[i].IP != w.IPAddress || got[i].Port != w.Port || got[i].ResponseTime != w.ResponseTime ||
				got[i].Alive != w.Alive || got[i].LatestCheck.IsZero() == w.LatestCheck.Valid || (w.LatestCheck.Valid && !got[i].LatestCheck.Equal(w.LatestCheck.Time)) {
				t.Fatalf("recent limit %d row %d: %+v, want %+v", limit, i, got[i], w)
			}
		}
	}

	// A warm view is one statement: the joined read.
	log.take()
	warm, err := service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	statements := log.take()
	if warm.Status != StatusFresh || len(statements) != 1 || !strings.Contains(statements[0], "LEFT JOIN dashboard_snapshots") {
		t.Fatalf("warm view status=%s statements=%v", warm.Status, statements)
	}

	// The live figures follow the meter without a rebuild.
	if err := db.Exec("UPDATE workspace_usage_periods SET checks = 9 WHERE workspace_id = ? AND period_start = ?", owner.ID, period).Error; err != nil {
		t.Fatal(err)
	}
	if live, err := service.View(context.Background(), owner.ID); err != nil || live.ChecksThisMonth != 9 || live.TotalChecks != 49 {
		t.Fatalf("live figures: %+v %v", live, err)
	}
}

func TestStaleViewServesOldPartsWhileCooldownRefusesClaimsPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0]
	log := &statementLog{Interface: logger.Default.LogMode(logger.Silent)}
	service := newTestService(db.Session(&gorm.Session{Logger: log}), Config{})
	defer service.Stop()
	if _, err := service.View(context.Background(), owner.ID); err != nil {
		t.Fatal(err)
	}
	// Age the snapshot to five minutes; the claim cooldown is still running.
	if err := db.Exec(`UPDATE dashboard_snapshots SET computed_at = now() - interval '5 minutes', next_eligible_at = now() + interval '1 minute' WHERE workspace_id = ?`, owner.ID).Error; err != nil {
		t.Fatal(err)
	}
	log.take()
	view, err := service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != StatusStale || view.AliveCount() == nil || len(view.FastestAliveProxies(5)) == 0 {
		t.Fatalf("stale view: %+v", view)
	}
	for _, statement := range log.take() {
		if strings.Contains(statement, "UPDATE dashboard_snapshots") {
			t.Fatal("an ineligible row was claimed", statement)
		}
	}
	// Once eligible, a stale row is rebuilt and the request sees the publish.
	if err := db.Exec(`UPDATE dashboard_snapshots SET next_eligible_at = NULL WHERE workspace_id = ?`, owner.ID).Error; err != nil {
		t.Fatal(err)
	}
	if view, err := service.View(context.Background(), owner.ID); err != nil || view.Status != StatusFresh {
		t.Fatalf("eligible stale view: %+v %v", view, err)
	}
}

func TestGenerationChangeServesThePreviousHealthAsOutdatedPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0]
	service := newTestService(db, Config{})
	defer service.Stop()
	built, err := service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Another instance commits a projection change.
	if err := db.Model(&domain.Workspace{}).Where("id = ?", owner.ID).Update("checker_generation", gorm.Expr("checker_generation + 1")).Error; err != nil {
		t.Fatal(err)
	}
	// The cooldown keeps the row from being rebuilt, so the view is pending
	// and serves the previous health, marked outdated.
	view, err := service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != StatusPending || !view.HealthOutdated || view.CheckerSettingsPending || view.TotalScraped() == nil ||
		view.AliveCount() == nil || *view.AliveCount() != *built.AliveCount() || len(view.FastestAliveProxies(5)) != len(built.FastestAliveProxies(5)) ||
		view.HealthCollectedAt == nil || !view.HealthCollectedAt.Equal(*built.HealthCollectedAt) {
		t.Fatalf("view after a generation change: %+v", view)
	}

	// A workspace dirty before its first build has no health to show.
	dirty := workspaces[3]
	view, err = service.View(context.Background(), dirty.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != StatusFresh || !view.CheckerSettingsPending || view.HealthOutdated || view.FastestAliveProxies(5) != nil || view.AliveCount() != nil {
		t.Fatalf("dirty workspace view: %+v", view)
	}
	var stored struct{ HasHealth bool }
	if err := db.Raw("SELECT health IS NOT NULL AS has_health FROM dashboard_snapshots WHERE workspace_id = ?", dirty.ID).Scan(&stored).Error; err != nil || stored.HasHealth {
		t.Fatalf("a dirty build stored health: %+v %v", stored, err)
	}
}

// While checker settings apply, a rebuild refreshes the inventory and keeps
// the previous health; once they are applied, the next build replaces it.
func TestBuildWhileSettingsApplyKeepsThePreviousHealthPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0]
	service := newTestService(db, Config{})
	defer service.Stop()
	built, err := service.View(context.Background(), owner.ID)
	if err != nil || built.HealthOutdated || built.AliveCount() == nil {
		t.Fatalf("first build: %+v %v", built, err)
	}
	type row struct {
		Health            []byte
		CheckerGeneration uint64
		HealthCollectedAt time.Time
		CollectedAt       time.Time
	}
	readRow := func() row {
		t.Helper()
		var stored row
		if err := db.Raw("SELECT health, checker_generation, health_collected_at, collected_at FROM dashboard_snapshots WHERE workspace_id = ?", owner.ID).Scan(&stored).Error; err != nil {
			t.Fatal(err)
		}
		return stored
	}
	before := readRow()
	if len(before.Health) == 0 || !before.HealthCollectedAt.Equal(before.CollectedAt) {
		t.Fatalf("first build row: %+v", before)
	}

	makeEligible := func() {
		t.Helper()
		if err := db.Exec(`UPDATE dashboard_snapshots SET computed_at = computed_at - interval '1 minute', claimed_at = claimed_at - interval '1 minute',
	collected_at = collected_at - interval '1 minute', health_collected_at = health_collected_at - interval '1 minute', next_eligible_at = NULL
	WHERE workspace_id = ?`, owner.ID).Error; err != nil {
			t.Fatal(err)
		}
	}
	// New settings are saved: the workspace is dirty until they apply.
	if err := db.Model(&domain.Workspace{}).Where("id = ?", owner.ID).Update("checker_dirty", true).Error; err != nil {
		t.Fatal(err)
	}
	makeEligible()
	before = readRow()
	view, err := service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	after := readRow()
	if view.Status != StatusFresh || !view.CheckerSettingsPending || !view.HealthOutdated || view.AliveCount() == nil || *view.AliveCount() != *built.AliveCount() {
		t.Fatalf("view while settings apply: %+v", view)
	}
	if !after.CollectedAt.After(before.CollectedAt) || string(after.Health) != string(before.Health) ||
		after.CheckerGeneration != before.CheckerGeneration || !after.HealthCollectedAt.Equal(before.HealthCollectedAt) {
		t.Fatalf("a build while settings apply replaced the previous health: before %+v after %+v", before, after)
	}
	if !view.HealthCollectedAt.Equal(before.HealthCollectedAt) || !view.CollectedAt.After(*view.HealthCollectedAt) {
		t.Fatalf("collection times while settings apply: %+v", view)
	}

	// The settings are applied under a new generation; the next view builds
	// current health at once instead of waiting out the cooldown.
	if err := db.Model(&domain.Workspace{}).Where("id = ?", owner.ID).
		Updates(map[string]any{"checker_dirty": false, "checker_generation": gorm.Expr("checker_generation + 1")}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`UPDATE dashboard_snapshots SET next_eligible_at = NULL WHERE workspace_id = ?`, owner.ID).Error; err != nil {
		t.Fatal(err)
	}
	view, err = service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	current := readRow()
	if view.Status != StatusFresh || view.HealthOutdated || view.CheckerSettingsPending || view.AliveCount() == nil ||
		current.CheckerGeneration != before.CheckerGeneration+1 || !current.HealthCollectedAt.Equal(current.CollectedAt) {
		t.Fatalf("view after the settings applied: %+v row %+v", view, current)
	}
}

func TestBuildDuringGenerationChangeStoresNoHealthPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0]
	changed := false
	hook := &generationRaceLogger{Interface: logger.Default.LogMode(logger.Silent), match: "AS elite_proxies", change: func() {
		changed = true
		if err := db.Model(&domain.Workspace{}).Where("id = ?", owner.ID).Update("checker_generation", gorm.Expr("checker_generation + 1")).Error; err != nil {
			t.Error(err)
		}
	}}
	service := newTestService(db.Session(&gorm.Session{Logger: hook}), Config{StatementConcurrency: 1})
	defer service.Stop()
	view, err := service.View(context.Background(), owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || view.AliveCount() != nil || view.HealthOutdated || view.TotalScraped() == nil || view.Status != StatusPending {
		t.Fatalf("health computed across a generation change was served: changed=%v view=%+v", changed, view)
	}
	var stored struct {
		HasHealth         bool
		CheckerGeneration *uint64
	}
	if err := db.Raw("SELECT health IS NOT NULL AS has_health, checker_generation FROM dashboard_snapshots WHERE workspace_id = ?", owner.ID).Scan(&stored).Error; err != nil || stored.HasHealth || stored.CheckerGeneration != nil {
		t.Fatalf("stored build: %+v %v", stored, err)
	}
}

type generationRaceLogger struct {
	logger.Interface
	once   sync.Once
	match  string
	change func()
}

func (l *generationRaceLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if sql, _ := fc(); strings.Contains(sql, l.match) {
		l.once.Do(l.change)
	}
}

func TestDeletedWorkspaceIsAnErrorPostgres(t *testing.T) {
	db, _ := snapshotFixture(t)
	owner := domain.Workspace{Name: "Doomed"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	service := newTestService(db, Config{})
	defer service.Stop()
	if _, err := service.View(context.Background(), owner.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DELETE FROM workspaces WHERE id = ?", owner.ID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.View(context.Background(), owner.ID); !errors.Is(err, errWorkspaceNotFound) {
		t.Fatalf("view of a deleted workspace: %v", err)
	}
	var rows int64
	if err := db.Model(&domain.DashboardSnapshot{}).Where("workspace_id = ?", owner.ID).Count(&rows).Error; err != nil || rows != 0 {
		t.Fatalf("snapshot rows of a deleted workspace=%d err=%v", rows, err)
	}
}

func TestClaimRulesPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	store := NewStore(db)
	ctx := context.Background()
	a, b := workspaces[0].ID, workspaces[1].ID
	for _, id := range []uint{a, b} {
		if inserted, err := store.ensureRow(ctx, id); err != nil || !inserted {
			t.Fatal("insert row", inserted, err)
		}
	}
	first, admitted, err := store.claim(ctx, a, 1, time.Minute)
	if err != nil || !admitted {
		t.Fatal("first claim", admitted, err)
	}
	if _, admitted, err := store.claim(ctx, a, 4, time.Minute); err != nil || admitted {
		t.Fatal("an active lease admitted a second claim", admitted, err)
	}
	// The cluster cap counts active leases of every workspace.
	if _, admitted, err := store.claim(ctx, b, 1, time.Minute); err != nil || admitted {
		t.Fatal("the cluster cap admitted a claim", admitted, err)
	}
	// A publish with another epoch changes nothing.
	if published, err := store.publish(ctx, a, first.Epoch+1000, []byte(`{"inventory":{}}`), []byte(`{}`), 1); err != nil || published {
		t.Fatal("a foreign epoch published", published, err)
	}
	if err := store.fail(ctx, a, first.Epoch); err != nil {
		t.Fatal(err)
	}
	var row struct {
		FailureCount int
		Backoff      float64
		LeaseActive  bool
	}
	readRow := func() {
		t.Helper()
		if err := db.Raw(`SELECT failure_count, EXTRACT(EPOCH FROM next_eligible_at - now())::float8 AS backoff, lease_until IS NOT NULL AS lease_active
			FROM dashboard_snapshots WHERE workspace_id = ?`, a).Scan(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	readRow()
	if row.FailureCount != 1 || row.LeaseActive || row.Backoff < 25 || row.Backoff > 31 {
		t.Fatalf("after the first failure: %+v", row)
	}
	// Failures back off exponentially up to ten minutes.
	for i := 0; i < 8; i++ {
		if err := db.Exec("UPDATE dashboard_snapshots SET next_eligible_at = NULL WHERE workspace_id = ?", a).Error; err != nil {
			t.Fatal(err)
		}
		next, admitted, err := store.claim(ctx, a, 4, time.Minute)
		if err != nil || !admitted {
			t.Fatal("claim after backoff", i, admitted, err)
		}
		if err := db.Exec("UPDATE dashboard_snapshots SET next_eligible_at = NULL WHERE workspace_id = ?", a).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.fail(ctx, a, next.Epoch); err != nil {
			t.Fatal(err)
		}
	}
	readRow()
	if row.FailureCount != 9 || row.Backoff < 590 || row.Backoff > 601 {
		t.Fatalf("after nine failures: %+v", row)
	}
	// A failure of a released lease is not recorded twice.
	if err := store.fail(ctx, a, first.Epoch); err != nil {
		t.Fatal(err)
	}
	readRow()
	if row.FailureCount != 9 {
		t.Fatalf("a released lease counted a failure: %+v", row)
	}

	// Release clears a lease without counting a failure.
	if err := db.Exec("UPDATE dashboard_snapshots SET next_eligible_at = NULL WHERE workspace_id = ?", b).Error; err != nil {
		t.Fatal(err)
	}
	granted, admitted, err := store.claim(ctx, b, 4, time.Minute)
	if err != nil || !admitted {
		t.Fatal("claim b", admitted, err)
	}
	if err := store.release(ctx, b, granted.Epoch); err != nil {
		t.Fatal(err)
	}
	var released struct {
		FailureCount int
		LeaseActive  bool
	}
	if err := db.Raw("SELECT failure_count, lease_until IS NOT NULL AS lease_active FROM dashboard_snapshots WHERE workspace_id = ?", b).Scan(&released).Error; err != nil || released.FailureCount != 0 || released.LeaseActive {
		t.Fatalf("released lease: %+v %v", released, err)
	}
	// Epochs come from the sequence and only grow.
	if granted.Epoch <= first.Epoch {
		t.Fatalf("epochs %d then %d", first.Epoch, granted.Epoch)
	}
}

func TestClaimWaitsAtMostTheLockTimeoutPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	store := NewStore(db)
	ctx := context.Background()
	owner := workspaces[0].ID
	if inserted, err := store.ensureRow(ctx, owner); err != nil || !inserted {
		t.Fatal(inserted, err)
	}
	holder := db.Begin()
	defer holder.Rollback()
	if err := holder.Exec("SELECT pg_advisory_xact_lock(?)", admissionLockKey).Error; err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, admitted, err := store.claim(ctx, owner, 4, time.Minute); err != nil || admitted {
		t.Fatal("claim under a held admission lock", admitted, err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("claim waited %s", elapsed)
	}
	// The row insert waits for the workspace row the same way.
	other := workspaces[1].ID
	locker := db.Begin()
	defer locker.Rollback()
	if err := locker.Exec("SELECT id FROM workspaces WHERE id = ? FOR UPDATE", other).Error; err != nil {
		t.Fatal(err)
	}
	if inserted, err := store.ensureRow(ctx, other); err != nil || inserted {
		t.Fatal("insert under a locked workspace row", inserted, err)
	}
}

func TestLeaseHolderElsewhereIsPolledThenServedPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0].ID
	other := newTestService(db, Config{})
	defer other.Stop()
	store := other.store
	if _, err := store.ensureRow(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	// Another instance holds the lease; this one waits for its publish.
	granted, admitted, err := store.claim(context.Background(), owner, 4, time.Minute)
	if err != nil || !admitted {
		t.Fatal(admitted, err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		result, err := store.build(context.Background(), owner, 2)
		if err == nil {
			var encoded, encodedHealth []byte
			if encoded, encodedHealth, err = encodeSnapshot(result.payload, result.health); err == nil {
				_, err = store.publish(context.Background(), owner, granted.Epoch, encoded, encodedHealth, result.generation)
			}
		}
		if err != nil {
			t.Error(err)
		}
	}()
	service := newTestService(db, Config{})
	defer service.Stop()
	view, err := service.View(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != StatusFresh || view.AliveCount() == nil {
		t.Fatalf("view after waiting for another instance: %+v", view)
	}
}

func TestStopClearsLeasesWithoutCountingFailuresPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0].ID
	blocked := make(chan struct{})
	hook := &generationRaceLogger{Interface: logger.Default.LogMode(logger.Silent), match: "AS elite_proxies", change: func() {
		close(blocked)
		time.Sleep(300 * time.Millisecond)
	}}
	service := newTestService(db.Session(&gorm.Session{Logger: hook}), Config{StatementConcurrency: 1})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { service.Run(ctx); close(done) }()
	viewed := make(chan error, 1)
	go func() { _, err := service.View(context.Background(), owner); viewed <- err }()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("build never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not join the build")
	}
	if err := <-viewed; err != nil {
		t.Fatal(err)
	}
	var row struct {
		FailureCount int
		LeaseActive  bool
		Published    bool
	}
	if err := db.Raw("SELECT failure_count, lease_until IS NOT NULL AS lease_active, computed_at IS NOT NULL AS published FROM dashboard_snapshots WHERE workspace_id = ?", owner).Scan(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.FailureCount != 0 || row.LeaseActive || row.Published {
		t.Fatalf("after Stop: %+v", row)
	}
	// A stopped service claims nothing.
	if view, err := service.View(context.Background(), owner); err != nil || view.Rebuilding || view.Status != StatusPending {
		t.Fatalf("view from a stopped service: %+v %v", view, err)
	}
}

func TestRetentionDeletesOnlyIdleRowsPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	store := NewStore(db)
	ctx := context.Background()
	for _, workspace := range workspaces {
		if _, err := store.ensureRow(ctx, workspace.ID); err != nil {
			t.Fatal(err)
		}
	}
	old := "now() - interval '8 days'"
	for i, update := range []string{
		"created_at = " + old, // idle: deleted
		"created_at = " + old + ", lease_until = now() + interval '1 minute'",       // active lease: kept
		"created_at = " + old + ", next_eligible_at = now() + interval '5 minutes'", // backing off: kept
		"created_at = " + old + ", computed_at = now() - interval '1 day'",          // recently published: kept
	} {
		if err := db.Exec("UPDATE dashboard_snapshots SET "+update+" WHERE workspace_id = ?", workspaces[i].ID).Error; err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := store.DeleteExpired(ctx)
	if err != nil || deleted != 1 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	var kept []uint
	if err := db.Model(&domain.DashboardSnapshot{}).Order("workspace_id").Pluck("workspace_id", &kept).Error; err != nil {
		t.Fatal(err)
	}
	if len(kept) != 3 || kept[0] != workspaces[1].ID {
		t.Fatalf("kept rows %v", kept)
	}
	// A held admission lock skips the batch instead of failing the routine.
	holder := db.Begin()
	defer holder.Rollback()
	if err := holder.Exec("SELECT pg_advisory_xact_lock(?)", admissionLockKey).Error; err != nil {
		t.Fatal(err)
	}
	if deleted, err := store.DeleteExpired(ctx); err != nil || deleted != 0 {
		t.Fatalf("retention under a held lock: %d %v", deleted, err)
	}
}

// A build that reaches a blocked statement is canceled at its deadline: the
// statement stops on the server, the failure is recorded with backoff, and
// the previous snapshot keeps being served.
func TestBuildDeadlineCancelsBlockedStatementsPostgres(t *testing.T) {
	application := fmt.Sprintf("dashboard_deadline_%d", time.Now().UnixNano())
	if dsn := os.Getenv("MAGPIE_TEST_POSTGRES_DSN"); dsn != "" {
		t.Setenv("MAGPIE_TEST_POSTGRES_DSN", dsn+" application_name="+application)
	}
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0].ID
	service := newTestService(db, Config{BuildDeadline: time.Second})
	defer service.Stop()
	if view, err := service.View(context.Background(), owner); err != nil || view.Status != StatusFresh {
		t.Fatalf("first view: %+v %v", view, err)
	}
	var published time.Time
	if err := db.Raw("SELECT computed_at FROM dashboard_snapshots WHERE workspace_id = ?", owner).Scan(&published).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE dashboard_snapshots SET computed_at = computed_at - interval '2 minutes', next_eligible_at = NULL WHERE workspace_id = ?", owner).Error; err != nil {
		t.Fatal(err)
	}
	blocker := db.Begin()
	defer blocker.Rollback()
	if err := blocker.Exec(`LOCK TABLE proxy_latest_statistics IN ACCESS EXCLUSIVE MODE`).Error; err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	view, err := service.View(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("the request waited %s", elapsed)
	}
	if view.Status != StatusStale || view.AliveCount() == nil {
		t.Fatalf("view during a blocked build: %+v", view)
	}
	var row struct {
		FailureCount int
		LeaseActive  bool
		Unchanged    bool
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := db.Raw(`SELECT failure_count, lease_until IS NOT NULL AS lease_active, computed_at = CAST(? AS timestamptz) - interval '2 minutes' AS unchanged
			FROM dashboard_snapshots WHERE workspace_id = ?`, published, owner).Scan(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.FailureCount > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if row.FailureCount != 1 || row.LeaseActive || !row.Unchanged {
		t.Fatalf("after the deadline: %+v", row)
	}
	var waiting int64
	if err := db.Raw(`SELECT count(*) FROM pg_stat_activity WHERE application_name = ? AND state = 'active' AND wait_event_type = 'Lock'`, application).Scan(&waiting).Error; err != nil || waiting != 0 {
		t.Fatalf("blocked build statements remain: %d %v", waiting, err)
	}
	if view, err := service.View(context.Background(), owner); err != nil || view.Status != StatusStale {
		t.Fatalf("view after the failure: %+v %v", view, err)
	}
}
