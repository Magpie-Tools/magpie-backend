package database

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestLatestEvidenceUsesConflictKeyLockOrder(t *testing.T) {
	stats := []domain.ProxyStatistic{
		{ID: 1, ProxyID: 8, ProtocolID: 2},
		{ID: 2, ProxyID: 3, ProtocolID: 1},
		{ID: 3, ProxyID: 8, ProtocolID: 1},
		{ID: 4, ProxyID: 9, ProtocolID: 2, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: 4, ConfigKey: "a"}, {WorkspaceID: 2, ConfigKey: "z"}, {WorkspaceID: 2, ConfigKey: "A"}}},
		{ID: 5, ProxyID: 3, ProtocolID: 2, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: 2, ConfigKey: "A"}}},
		{ID: 6, ProxyID: 3, ProtocolID: 1, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: 2, ConfigKey: "A"}}},
	}
	want := []string{"0//3/1", "0//8/1", "0//8/2", "2/A/3/1", "2/A/3/2", "2/A/9/2", "2/z/9/2", "4/a/9/2"}
	for iteration := 0; iteration < 20; iteration++ {
		entries, legacy := latestProxyStatusEntries(stats)
		var keys []string
		for _, entry := range entries {
			keys = append(keys, fmt.Sprintf("%d/%s/%d/%d", entry.WorkspaceID, entry.ConfigKey, entry.ProxyID, entry.ProtocolID))
		}
		if !reflect.DeepEqual(keys, want) || !reflect.DeepEqual(legacy, []uint64{3, 8}) {
			t.Fatalf("unstable shared-row lock order: latest=%v legacy=%v", keys, legacy)
		}
		for i, j := 0, len(stats)-1; i < j; i, j = i+1, j-1 {
			stats[i], stats[j] = stats[j], stats[i]
		}
	}
}

func TestStatisticsLatestWaitDoesNotPinImportUsagePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, b, proxy, judge := seedCheckerEvidenceTest(t, db)
	if err := db.Create(&domain.WorkspaceSubscription{WorkspaceID: a.ID, OverageMode: domain.WorkspaceOverageUnlimited}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true,
		CreatedAt: now, WorkspaceIDs: []uint{a.ID, b.ID}, EventStream: "checks", EventID: "1-0",
		CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "current", Alive: true}, {WorkspaceID: b.ID, ConfigKey: "current", Alive: true}}}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err != nil {
		t.Fatal(err)
	}
	if err := UpdateWorkspaceUsageActiveRoutes(db, a.ID, 1_000_000); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	DB = db.WithContext(ctx)
	held := db.WithContext(ctx).Begin()
	defer held.Rollback()
	var holderPID int
	if err := held.Raw("SELECT pg_backend_pid()").Scan(&holderPID).Error; err != nil {
		t.Fatal(err)
	}
	if err := held.Exec("SELECT proxy_id FROM proxy_latest_statistics WHERE workspace_id=? AND config_key='current' AND proxy_id=? AND protocol_id=1 FOR UPDATE", a.ID, proxy.ID).Error; err != nil {
		t.Fatal(err)
	}
	stat.CreatedAt, stat.EventID, stat.Attempt = now.Add(time.Second), "2-0", 2
	done := make(chan error, 1)
	go func() { done <- InsertProxyStatistics(ctx, []domain.ProxyStatistic{stat}, 1) }()
	waitForStatisticsLatestBlock(t, db, holderPID)
	// This is the real import path, including the subscription capacity lock,
	// derived rows and authoritative route-count update. It must complete while
	// the earlier checker transaction is still stalled on a latest row.
	importDone := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{{IP: "192.0.2.2", Port: 8080}}, a.ID)
		importDone <- err
	}()
	select {
	case err := <-importDone:
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("real import completed in %.3fms while checker latest-row lock remained held", float64(time.Since(start).Microseconds())/1000)
	case <-time.After(time.Second):
		t.Fatal("blocked latest evidence retained a usage row needed by import")
	}
	var usage domain.WorkspaceUsagePeriod
	if err := db.First(&usage, "workspace_id=?", a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if usage.ActiveRoutes != 2 || usage.PeakActiveRoutes != 1_000_000 || usage.CheckAttempts != 1 {
		t.Fatalf("uncommitted checker work leaked or import capacity was lost: %+v", usage)
	}
	var uncommitted int64
	if err := db.Model(&domain.ProxyStatisticEvent{}).Where("stream='checks' AND event_id='2-0'").Count(&uncommitted).Error; err != nil || uncommitted != 0 {
		t.Fatalf("uncommitted event became visible: count=%d err=%v", uncommitted, err)
	}
	if err := held.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, owner := range []uint{a.ID, b.ID} {
		usage = domain.WorkspaceUsagePeriod{}
		if err := db.First(&usage, "workspace_id=?", owner).Error; err != nil {
			t.Fatal(err)
		}
		if usage.CheckAttempts != 4 {
			t.Fatalf("workspace %d counted %d attempts, want 4", owner, usage.CheckAttempts)
		}
	}
	usage = domain.WorkspaceUsagePeriod{}
	if err := db.First(&usage, "workspace_id=?", a.ID).Error; err != nil || usage.ActiveRoutes != 2 || usage.PeakActiveRoutes != 1_000_000 {
		t.Fatalf("tail check delta overwrote authoritative capacity: %+v err=%v", usage, err)
	}
}

func waitForStatisticsLatestBlock(t *testing.T, db *gorm.DB, holderPID int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int64
		if err := db.Raw(`SELECT count(*) FROM pg_stat_activity
WHERE datname=current_database() AND ?=ANY(pg_blocking_pids(pid))
AND query LIKE 'INSERT INTO "proxy_latest_statistics"%'`, holderPID).Scan(&waiting).Error; err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("statistics transaction did not reach the held latest row")
}

func TestStatisticsTailUsageFailureRollsBackAllEffectsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond), WorkspaceIDs: []uint{a.ID}, EventStream: "checks", EventID: "1-0",
		CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "current", Alive: true}}}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err != nil {
		t.Fatal(err)
	}
	var before domain.ProxyLatestStatistic
	if err := db.First(&before, "workspace_id=?", a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE FUNCTION reject_tail_usage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (SELECT max(statistic_id) FROM proxy_latest_statistics) <> 2
     OR (SELECT sum(version) FROM proxy_reputation_refreshes) <> 2 THEN
    RAISE EXCEPTION 'usage ran before latest or reputation writes';
  END IF;
  RAISE EXCEPTION 'injected final usage failure';
END $$;
CREATE TRIGGER reject_tail_usage BEFORE INSERT OR UPDATE ON workspace_usage_periods
FOR EACH ROW EXECUTE FUNCTION reject_tail_usage()`).Error; err != nil {
		t.Fatal(err)
	}
	stat.EventID, stat.CreatedAt = "2-0", stat.CreatedAt.Add(time.Second)
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err == nil || !strings.Contains(err.Error(), "injected final usage failure") {
		t.Fatalf("expected failure at the final usage write, got %v", err)
	}
	assertStatisticsTailState(t, db, a.ID, proxy.ID, 1, 1, 1)
	var after domain.ProxyLatestStatistic
	if err := db.First(&after, "workspace_id=?", a.ID).Error; err != nil || after.StatisticID != before.StatisticID || !after.CheckedAt.Equal(before.CheckedAt) {
		t.Fatalf("late failure retained latest evidence: before=%+v after=%+v err=%v", before, after, err)
	}
	if err := db.Exec("DROP TRIGGER reject_tail_usage ON workspace_usage_periods").Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err != nil {
			t.Fatal(err)
		}
	}
	assertStatisticsTailState(t, db, a.ID, proxy.ID, 2, 2, 2)
}

func assertStatisticsTailState(t *testing.T, db *gorm.DB, workspaceID uint, proxyID uint64, events, attempts int64, reputationVersion uint64) {
	t.Helper()
	for _, model := range []any{&domain.ProxyStatistic{}, &domain.ProxyStatisticEvent{}} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil || count != events {
			t.Fatalf("%T rows=%d want=%d err=%v", model, count, events, err)
		}
	}
	var daily int64
	if err := db.Model(&domain.ProxyDailyCheck{}).Select("COALESCE(SUM(checks_count),0)").Where("proxy_id=?", proxyID).Scan(&daily).Error; err != nil || daily != events {
		t.Fatalf("daily count=%d want=%d err=%v", daily, events, err)
	}
	var usage domain.WorkspaceUsagePeriod
	if err := db.First(&usage, "workspace_id=?", workspaceID).Error; err != nil || usage.CheckAttempts != uint64(attempts) {
		t.Fatalf("usage=%+v want attempts=%d err=%v", usage, attempts, err)
	}
	var job domain.ProxyReputationRefresh
	if err := db.First(&job, "proxy_id=?", proxyID).Error; err != nil || job.Version != reputationVersion {
		t.Fatalf("reputation=%+v want version=%d err=%v", job, reputationVersion, err)
	}
}

func TestStatisticsConcurrentOppositeOrderOverlapPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, b, proxy, judge := seedCheckerEvidenceTest(t, db)
	owners := []uint{a.ID, b.ID}
	for i := 2; i < 8; i++ {
		owner := domain.Workspace{Name: fmt.Sprintf("Lock order owner %d", i)}
		if err := db.Create(&owner).Error; err != nil {
			t.Fatal(err)
		}
		owners = append(owners, owner.ID)
	}
	const routes = 100
	if err := db.Exec(`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type) SELECT n,'192.0.2.1','192.0.2.1',8080,'N/A','N/A' FROM generate_series(2,?) n`, routes).Error; err != nil {
		t.Fatal(err)
	}
	if proxy.ID != 1 {
		t.Fatal("unexpected seed proxy id", proxy.ID)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	makeStats := func(offset int, reverse bool) []domain.ProxyStatistic {
		stats := make([]domain.ProxyStatistic, routes)
		for i := range stats {
			id := i + 1
			if reverse {
				id = routes - i
			}
			stat := domain.ProxyStatistic{ProxyID: uint64(id), ProtocolID: 1, JudgeID: judge.ID, Alive: true,
				CreatedAt: now.AddDate(0, 0, offset), WorkspaceIDs: append([]uint(nil), owners...), EventStream: "checks", EventID: fmt.Sprintf("%d-%d", 1000+offset, id)}
			for j := range owners {
				owner := owners[j]
				if reverse {
					owner = owners[len(owners)-1-j]
				}
				stat.CheckEvidence = append(stat.CheckEvidence, domain.WorkspaceCheckEvidence{WorkspaceID: owner, ConfigKey: "current", Alive: true})
			}
			stats[i] = stat
		}
		return stats
	}
	if err := InsertProxyStatistics(context.Background(), makeStats(-2, false), 100); err != nil {
		t.Fatal(err)
	}
	// Different days avoid the earlier daily-counter lock serializing the two
	// batches. A tiny row trigger keeps their overlapping latest upserts live
	// together, exercising the explicit conflict-key acquisition order.
	if err := db.Exec(`CREATE FUNCTION slow_latest_order() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_sleep(0.0001); RETURN NEW; END $$;
CREATE TRIGGER slow_latest_order BEFORE UPDATE ON proxy_latest_statistics
FOR EACH ROW EXECUTE FUNCTION slow_latest_order()`).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gate := make(chan struct{})
	errors := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for i := 0; i < 2; i++ {
		stats := makeStats(i-1, i == 1)
		go func() {
			ready.Done()
			<-gate
			errors <- InsertProxyStatistics(ctx, stats, 100)
		}()
	}
	ready.Wait()
	close(gate)
	for i := 0; i < 2; i++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	for _, owner := range owners {
		var usage domain.WorkspaceUsagePeriod
		if err := db.First(&usage, "workspace_id=?", owner).Error; err != nil || usage.CheckAttempts != routes*3 {
			t.Fatalf("owner %d usage=%+v err=%v", owner, usage, err)
		}
	}
	var latest int64
	if err := db.Model(&domain.ProxyLatestStatistic{}).Where("checked_at=?", now).Count(&latest).Error; err != nil || latest != routes*8 {
		t.Fatalf("newest evidence rows=%d want=%d err=%v", latest, routes*8, err)
	}
	for _, model := range []any{&domain.ProxyStatistic{}, &domain.ProxyStatisticEvent{}, &domain.ProxyDailyCheck{}} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil || count != routes*3 {
			t.Fatalf("%T count=%d want=%d err=%v", model, count, routes*3, err)
		}
	}
}
