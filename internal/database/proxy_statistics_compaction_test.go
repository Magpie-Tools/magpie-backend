package database

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/domain"
)

func TestStatisticLedgerRetirementPreservesReplayProtection(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			var db *gorm.DB
			if postgres {
				db = setupProxyIngestionPostgresTest(t)
			} else {
				db = openDailyChecksSQLite(t)
				old := DB
				DB = db
				t.Cleanup(func() { DB = old })
				if err := db.AutoMigrate(&domain.ProxyStatisticEvent{}, &domain.ProxyStatisticStream{}); err != nil {
					t.Fatal(err)
				}
			}
			stats := []domain.ProxyStatistic{{EventStream: "checks", EventID: "9-1"}, {EventStream: "checks", EventID: "10-0"}, {EventStream: "checks", EventID: "10-2"}}
			if accepted, err := acceptProxyStatisticEvents(db, stats); err != nil || len(accepted) != 3 {
				t.Fatalf("accept: %d %v", len(accepted), err)
			}
			deleted, err := RetireProxyStatisticEvents(context.Background(), "checks", "10-0", 100)
			if err != nil || deleted != 1 {
				t.Fatalf("retire: %d %v", deleted, err)
			}
			stats = append(stats, domain.ProxyStatistic{EventStream: "checks", EventID: "10-1"})
			if accepted, err := acceptProxyStatisticEvents(db, stats); err != nil || len(accepted) != 1 || accepted[0].EventID != "10-1" {
				t.Fatalf("replay: %+v %v", accepted, err)
			}
			if _, err := RetireProxyStatisticEvents(context.Background(), "checks", "9-0", 100); err != nil {
				t.Fatal(err)
			}
			var floor domain.ProxyStatisticStream
			if err := db.First(&floor, "stream = ?", "checks").Error; err != nil || floor.ReplayBefore != ProxyStatisticEventSortID("10-0") {
				t.Fatalf("floor moved back: %+v %v", floor, err)
			}
			// Upgrade a legacy ledger row without losing its replay fence.
			if err := db.Create(&domain.ProxyStatisticEvent{Stream: "checks", EventID: "8-1"}).Error; err != nil {
				t.Fatal(err)
			}
			if _, err := RetireProxyStatisticEvents(context.Background(), "checks", "10-3", 2); err != nil {
				t.Fatal(err)
			}
			if accepted, err := acceptProxyStatisticEvents(db, []domain.ProxyStatistic{{EventStream: "checks", EventID: "8-1"}}); err != nil || len(accepted) != 0 {
				t.Fatalf("legacy replay: %+v %v", accepted, err)
			}
		})
	}
}

func TestStatisticLedgerRetirementWaitsForIngestTransaction(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stat := domain.ProxyStatistic{EventStream: "checks", EventID: "100-0"}
	if _, err := acceptProxyStatisticEvents(db, []domain.ProxyStatistic{stat}); err != nil {
		t.Fatal(err)
	}
	tx := db.WithContext(ctx).Begin()
	defer tx.Rollback()
	if _, err := acceptProxyStatisticEvents(tx, []domain.ProxyStatistic{stat}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := RetireProxyStatisticEvents(ctx, "checks", "101-0", 100); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("retirement bypassed active transaction: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retirement did not resume")
	}
	if accepted, err := acceptProxyStatisticEvents(db, []domain.ProxyStatistic{stat}); err != nil || len(accepted) != 0 {
		t.Fatalf("retired replay accepted: %v %v", accepted, err)
	}
}

func TestDailyCompactionPreservesTotalsAndBackfill(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	now := time.Now().UTC()
	stats := make([]domain.ProxyStatistic, 180)
	for i := range stats {
		stats[i] = domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, WorkspaceIDs: []uint{workspace.ID}, CreatedAt: now.AddDate(0, 0, -i-1)}
	}
	if err := InsertProxyStatistics(context.Background(), stats, 500); err != nil {
		t.Fatal(err)
	}
	before, err := queryDashboardCheckCountsFromDaily(workspace.ID, now.AddDate(0, 0, -7))
	if err != nil {
		t.Fatal(err)
	}
	for {
		rows, err := CompactProxyDailyChecks(context.Background(), now.AddDate(0, 0, -30), 17)
		if err != nil {
			t.Fatal(err)
		}
		if rows == 0 {
			break
		}
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return backfillProxyDailyChecksForProxyIDsTx(tx, []uint64{proxy.ID}, startOfUTCDay(now))
	}); err != nil {
		t.Fatal(err)
	}
	after, err := queryDashboardCheckCountsFromDaily(workspace.ID, now.AddDate(0, 0, -7))
	if err != nil || after != before || after.TotalChecks != 180 {
		t.Fatalf("counts changed: before=%+v after=%+v err=%v", before, after, err)
	}
	var rows int64
	if err := db.Model(&domain.ProxyDailyCheck{}).Count(&rows).Error; err != nil || rows > 31 {
		t.Fatalf("daily rows=%d err=%v", rows, err)
	}
	// Late completion for a compacted day counts once and is compacted later.
	stats[0].ID = 0
	stats[179].ID = 0
	if err := InsertProxyStatistics(context.Background(), stats[:1], 1); err != nil {
		t.Fatal(err)
	}
	if err := InsertProxyStatistics(context.Background(), stats[179:], 1); err != nil {
		t.Fatal(err)
	}
	if _, err := CompactProxyDailyChecks(context.Background(), now.AddDate(0, 0, -30), 100); err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return backfillProxyDailyChecksForProxyIDsTx(tx, []uint64{proxy.ID}, startOfUTCDay(now))
	}); err != nil {
		t.Fatal(err)
	}
	after, err = queryDashboardCheckCountsFromDaily(workspace.ID, now.AddDate(0, 0, -7))
	if err != nil || after.TotalChecks != 182 {
		t.Fatalf("late counts=%+v err=%v", after, err)
	}
	if err := db.Exec("DELETE FROM user_proxies WHERE proxy_id = ?", proxy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := DeleteOrphanProxies(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"proxy_daily_checks", "proxy_daily_check_proxy_backfills", "proxy_check_totals"} {
		if err := db.Table(table).Count(&rows).Error; err != nil || rows != 0 {
			t.Fatalf("orphan %s rows=%d err=%v", table, rows, err)
		}
	}
}

func TestIngestWaitingForRetirementReadsNewFloor(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := db.Create(&domain.ProxyStatisticStream{Stream: "checks"}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx := db.WithContext(ctx).Begin()
	defer tx.Rollback()
	if err := tx.Model(&domain.ProxyStatisticStream{}).Where("stream = ?", "checks").Update("replay_before", ProxyStatisticEventSortID("101-0")).Error; err != nil {
		t.Fatal(err)
	}
	type result struct {
		count int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		accepted, err := acceptProxyStatisticEvents(db.WithContext(ctx), []domain.ProxyStatistic{{EventStream: "checks", EventID: "100-0"}})
		done <- result{len(accepted), err}
	}()
	select {
	case result := <-done:
		t.Fatalf("ingest bypassed retirement lock: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || result.count != 0 {
			t.Fatalf("stale replay floor: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("ingest did not resume")
	}
}
