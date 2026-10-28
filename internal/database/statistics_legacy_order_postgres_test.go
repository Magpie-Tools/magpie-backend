package database

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestStatisticsLegacyAggregateFollowsSharedBarriersPostgres(t *testing.T) {
	for _, scenario := range []struct {
		Name        string
		Refresh     bool
		Attribution string
	}{{"shared-owner", true, "shared"}, {"shared-owner-missing-refresh", false, "shared"}, {"different-owners", true, "different"}, {"no-attribution", true, "none"}} {
		t.Run(scenario.Name, func(t *testing.T) {
			db := setupProxyIngestionPostgresTest(t)
			a, b, proxy, judge := seedCheckerEvidenceTest(t, db)
			month, _ := workspaceUsageMonth(time.Now().UTC())
			base := month.AddDate(0, 0, 10)
			first := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true,
				CreatedAt: base, WorkspaceIDs: []uint{a.ID}, EventStream: "checks", EventID: "10-1"}
			second := first
			second.ProtocolID, second.EventID = 2, "10-2"
			if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{first, second}, 2); err != nil {
				t.Fatal(err)
			}
			if !scenario.Refresh {
				if err := db.Migrator().DropTable(&domain.ProxyReputationRefresh{}); err != nil {
					t.Fatal(err)
				}
			}
			first.Alive, second.Alive = false, false
			first.CreatedAt, second.CreatedAt = base.AddDate(0, 0, 1), base.AddDate(0, 0, 2)
			first.EventID, second.EventID = "11-0", "12-0"
			if scenario.Attribution == "different" {
				second.WorkspaceIDs = []uint{b.ID}
			} else if scenario.Attribution == "none" {
				first.WorkspaceIDs, second.WorkspaceIDs = nil, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			held := db.WithContext(ctx).Begin()
			defer held.Rollback()
			var holderPID int
			if err := held.Raw("SELECT pg_backend_pid()").Scan(&holderPID).Error; err != nil {
				t.Fatal(err)
			}
			if err := held.Exec("SELECT proxy_id FROM proxy_overall_statuses WHERE proxy_id=? FOR UPDATE", proxy.ID).Error; err != nil {
				t.Fatal(err)
			}
			errors := make(chan error, 2)
			go func() { errors <- InsertProxyStatistics(ctx, []domain.ProxyStatistic{first}, 1) }()
			firstPID := waitForStatisticsBlockedPID(t, db, holderPID, "%INSERT INTO proxy_overall_statuses%")
			go func() { errors <- InsertProxyStatistics(ctx, []domain.ProxyStatistic{second}, 1) }()
			// The second latest protocol update has a different key/day. It must
			// wait at refresh or usage before its aggregate takes a snapshot.
			waitForStatisticsBlockedPID(t, db, firstPID, "%")
			if err := held.Commit().Error; err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := <-errors; err != nil {
					t.Fatal(err)
				}
			}
			for _, stat := range []domain.ProxyStatistic{first, second} {
				if err := InsertProxyStatistics(ctx, []domain.ProxyStatistic{stat}, 1); err != nil {
					t.Fatal(err)
				}
			}
			var overall domain.ProxyOverallStatus
			if err := db.First(&overall, "proxy_id=?", proxy.ID).Error; err != nil || overall.OverallAlive {
				t.Fatalf("legacy aggregate lost a committed dead protocol: %+v err=%v", overall, err)
			}
			var alive int64
			if err := db.Model(&domain.ProxyLatestStatistic{}).Where("workspace_id=0 AND alive").Count(&alive).Error; err != nil || alive != 0 {
				t.Fatalf("latest legacy protocol rows are not both dead: alive=%d err=%v", alive, err)
			}
			var history, ledger, daily int64
			if err := db.Model(&domain.ProxyStatistic{}).Count(&history).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&ledger).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&domain.ProxyDailyCheck{}).Select("SUM(checks_count)").Scan(&daily).Error; err != nil {
				t.Fatal(err)
			}
			if history != 4 || ledger != 4 || daily != 4 {
				t.Fatalf("legacy replay changed accounting: history=%d ledger=%d daily=%d", history, ledger, daily)
			}
			wantAttempts := uint64(4)
			if scenario.Attribution == "different" {
				wantAttempts = 3
			} else if scenario.Attribution == "none" {
				wantAttempts = 2
			}
			var usage domain.WorkspaceUsagePeriod
			if err := db.First(&usage, "workspace_id=?", a.ID).Error; err != nil || usage.CheckAttempts != wantAttempts {
				t.Fatalf("legacy usage=%+v want=%d err=%v", usage, wantAttempts, err)
			}
			if scenario.Refresh {
				var job domain.ProxyReputationRefresh
				if err := db.First(&job, "proxy_id=?", proxy.ID).Error; err != nil || job.Version != 3 {
					t.Fatalf("legacy replay changed reputation requests: %+v err=%v", job, err)
				}
			}
		})
	}
}

func TestStatisticsModernAndMixedBatchesKeepOneLockOrderPostgres(t *testing.T) {
	for _, refresh := range []bool{true, false} {
		t.Run(fmt.Sprintf("refresh-table-%t", refresh), func(t *testing.T) {
			db := setupProxyIngestionPostgresTest(t)
			a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
			other := domain.Proxy{IP: "192.0.2.2", Port: 8080}
			if err := db.Create(&other).Error; err != nil {
				t.Fatal(err)
			}
			month, _ := workspaceUsageMonth(time.Now().UTC())
			base := month.AddDate(0, 0, 10)
			legacy := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 2, JudgeID: judge.ID, Alive: true,
				CreatedAt: base, WorkspaceIDs: []uint{a.ID}, EventStream: "checks", EventID: "10-1"}
			scoped := domain.ProxyStatistic{ProxyID: other.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true,
				CreatedAt: base, WorkspaceIDs: []uint{a.ID}, EventStream: "checks", EventID: "10-2",
				CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "current", Alive: true}}}
			if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{legacy, scoped}, 2); err != nil {
				t.Fatal(err)
			}
			if !refresh {
				if err := db.Migrator().DropTable(&domain.ProxyReputationRefresh{}); err != nil {
					t.Fatal(err)
				}
			}
			// Pause the modern writer after its latest-row lock. A mixed writer
			// must block there before it owns usage, avoiding the inverse cycle
			// created by a legacy-only early-usage exception.
			if err := db.Exec(fmt.Sprintf(`CREATE FUNCTION pause_scoped_latest() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.workspace_id=%d AND NEW.proxy_id=%d THEN PERFORM pg_advisory_xact_lock(976126789); END IF; RETURN NEW; END $$;
CREATE TRIGGER pause_scoped_latest BEFORE UPDATE ON proxy_latest_statistics
FOR EACH ROW EXECUTE FUNCTION pause_scoped_latest()`, a.ID, other.ID)).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			held := db.WithContext(ctx).Begin()
			defer held.Rollback()
			var holderPID int
			if err := held.Raw("SELECT pg_backend_pid()").Scan(&holderPID).Error; err != nil {
				t.Fatal(err)
			}
			if err := held.Exec("SELECT pg_advisory_xact_lock(976126789)").Error; err != nil {
				t.Fatal(err)
			}
			modern := scoped
			modern.CreatedAt, modern.EventID = base.AddDate(0, 0, 2), "12-0"
			errors := make(chan error, 2)
			go func() { errors <- InsertProxyStatistics(ctx, []domain.ProxyStatistic{modern}, 1) }()
			modernPID := waitForStatisticsBlockedPID(t, db, holderPID, "%INSERT INTO \"proxy_latest_statistics\"%")
			legacy.CreatedAt, legacy.EventID, legacy.Alive = base.AddDate(0, 0, 1), "11-0", false
			scoped.CreatedAt, scoped.EventID, scoped.Alive = base.AddDate(0, 0, 1), "11-1", false
			scoped.CheckEvidence = []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "current", Alive: false}}
			mixed := []domain.ProxyStatistic{legacy, scoped}
			go func() { errors <- InsertProxyStatistics(ctx, mixed, 2) }()
			waitForStatisticsBlockedPID(t, db, modernPID, "%INSERT INTO \"proxy_latest_statistics\"%")
			if err := held.Commit().Error; err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := <-errors; err != nil {
					t.Fatal(err)
				}
			}
			for _, stats := range [][]domain.ProxyStatistic{{modern}, mixed} {
				if err := InsertProxyStatistics(ctx, stats, 2); err != nil {
					t.Fatal(err)
				}
			}
			var usage domain.WorkspaceUsagePeriod
			if err := db.First(&usage, "workspace_id=?", a.ID).Error; err != nil || usage.CheckAttempts != 5 {
				t.Fatalf("mixed replay changed usage: %+v err=%v", usage, err)
			}
			var latest domain.ProxyLatestStatistic
			if err := db.First(&latest, "workspace_id=? AND proxy_id=?", a.ID, other.ID).Error; err != nil || !latest.Alive || !latest.CheckedAt.Equal(modern.CreatedAt) {
				t.Fatalf("mixed older evidence replaced current result: %+v err=%v", latest, err)
			}
			var overall domain.ProxyOverallStatus
			if err := db.First(&overall, "proxy_id=?", proxy.ID).Error; err != nil || overall.OverallAlive {
				t.Fatalf("mixed legacy aggregate stale: %+v err=%v", overall, err)
			}
		})
	}
}

func waitForStatisticsBlockedPID(t *testing.T, db *gorm.DB, blockerPID int, queryPattern string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var pid int
		if err := db.Raw(`SELECT COALESCE((SELECT pid FROM pg_stat_activity
WHERE datname=current_database() AND ?=ANY(pg_blocking_pids(pid)) AND query LIKE ? LIMIT 1),0)`, blockerPID, queryPattern).Scan(&pid).Error; err != nil {
			t.Fatal(err)
		}
		if pid != 0 {
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("statistics did not block on PID %d for %s", blockerPID, queryPattern)
	return 0
}

func TestStatisticsReorderingKeepsStatementCountsPostgres(t *testing.T) {
	for _, mode := range []string{"modern", "legacy", "mixed"} {
		for _, refresh := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-refresh-%t", mode, refresh), func(t *testing.T) {
				db := setupProxyIngestionPostgresTest(t)
				a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
				if !refresh {
					if err := db.Migrator().DropTable(&domain.ProxyReputationRefresh{}); err != nil {
						t.Fatal(err)
					}
				}
				stats := []domain.ProxyStatistic{{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true,
					CreatedAt: time.Now().UTC(), WorkspaceIDs: []uint{a.ID}, EventStream: "checks", EventID: "1-0"}}
				if mode != "legacy" {
					stats[0].CheckEvidence = []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "current", Alive: true}}
				}
				if mode == "mixed" {
					legacy := stats[0]
					legacy.ProtocolID, legacy.EventID, legacy.CheckEvidence = 2, "1-1", nil
					stats = append(stats, legacy)
				}
				before := &statisticsCostQueryLogger{Interface: db.Logger, counts: make(map[string]int)}
				if err := insertStatisticsUsageBeforeLatestPrototype(context.Background(), db.Session(&gorm.Session{Logger: before}), stats); err != nil {
					t.Fatal(err)
				}
				for i := range stats {
					stats[i].EventID = fmt.Sprintf("2-%d", i)
					stats[i].CreatedAt = stats[i].CreatedAt.Add(time.Second)
				}
				after := &statisticsCostQueryLogger{Interface: db.Logger, counts: make(map[string]int)}
				DB = db.Session(&gorm.Session{Logger: after})
				if err := InsertProxyStatistics(context.Background(), stats, CalculateProxyStatisticBatchSize(len(stats))); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before.counts, after.counts) {
					t.Fatalf("transaction ordering added SQL: before=%v after=%v", before.counts, after.counts)
				}
				t.Logf("identical existing SQL counts: %v", after.counts)
			})
		}
	}
}
