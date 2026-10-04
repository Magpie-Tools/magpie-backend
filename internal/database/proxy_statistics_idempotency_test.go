package database

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestStatisticsReplayIsIdempotent(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var db *gorm.DB
			if dialect == "postgres" {
				db = setupProxyIngestionPostgresTest(t)
			} else {
				db = setupRotatingProxyTestDB(t)
				if err := db.AutoMigrate(&domain.ProxyStatisticEvent{}, &domain.ProxyDailyCheck{}, &domain.WorkspaceUsagePeriod{}, &domain.ProxyReputationRefresh{}); err != nil {
					t.Fatal(err)
				}
			}
			a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
			stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true,
				Attempt: 2, WorkspaceIDs: []uint{a.ID, a.ID}, CreatedAt: time.Now().UTC(), EventStream: "checks", EventID: "1-0"}
			for i := 0; i < 2; i++ {
				if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat, stat}, 100); err != nil {
					t.Fatal(err)
				}
			}
			assertStatisticsAccounting(t, db, proxy.ID, a.ID, 1, 3)
			var job domain.ProxyReputationRefresh
			if err := db.First(&job, "proxy_id = ?", proxy.ID).Error; err != nil || job.Version != 1 {
				t.Fatalf("replayed event added refresh work: job=%+v error=%v", job, err)
			}
			// History retention must not make a replay acceptable again.
			if err := db.Where("proxy_id = ?", proxy.ID).Delete(&domain.ProxyStatistic{}).Error; err != nil {
				t.Fatal(err)
			}
			if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err != nil {
				t.Fatal(err)
			}
			assertStatisticsAccounting(t, db, proxy.ID, a.ID, 0, 3)
			stat.EventStream = "other-checks"
			if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err != nil {
				t.Fatal(err)
			}
			assertStatisticsAccounting(t, db, proxy.ID, a.ID, 1, 6)
		})
	}
}

func assertStatisticsAccounting(t *testing.T, db *gorm.DB, proxyID uint64, workspaceID uint, history int64, attempts uint64) {
	t.Helper()
	var rows int64
	if err := db.Model(&domain.ProxyStatistic{}).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	var daily domain.ProxyDailyCheck
	if err := db.First(&daily, "proxy_id = ?", proxyID).Error; err != nil {
		t.Fatal(err)
	}
	var usage domain.WorkspaceUsagePeriod
	if err := db.First(&usage, "workspace_id = ?", workspaceID).Error; err != nil {
		t.Fatal(err)
	}
	if rows != history || daily.ChecksCount != int64(attempts/3) || usage.CheckAttempts != attempts {
		t.Fatalf("history=%d daily=%d attempts=%d, want %d/%d/%d", rows, daily.ChecksCount, usage.CheckAttempts, history, attempts/3, attempts)
	}
}

func TestStatisticEventRollsBackWithFailedPersistence(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.ProxyStatisticEvent{}, &domain.ProxyDailyCheck{}, &domain.WorkspaceUsagePeriod{}); err != nil {
		t.Fatal(err)
	}
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	stat := domain.ProxyStatistic{ProxyID: proxy.ID + 100, ProtocolID: 1, JudgeID: judge.ID, WorkspaceIDs: []uint{a.ID}, EventStream: "checks", EventID: "rollback"}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err == nil {
		t.Fatal("expected missing proxy foreign key error")
	}
	var count int64
	if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("failed transaction retained event identity: count=%d error=%v", count, err)
	}
	stat.ProxyID = proxy.ID
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentStatisticsReplayPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, Attempt: 2,
		WorkspaceIDs: []uint{a.ID}, CreatedAt: time.Now().UTC(), EventStream: "checks", EventID: "concurrent"}
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Go(func() { errors <- InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1) })
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertStatisticsAccounting(t, db, proxy.ID, a.ID, 1, 3)
}

func TestStatisticsUsageDoesNotRecountOrOverwriteActiveCapacity(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}); err != nil {
		t.Fatal(err)
	}
	a, _, proxy, _ := seedCheckerEvidenceTest(t, db)
	if err := UpdateWorkspaceUsageActiveRoutes(db, a.ID, 17); err != nil {
		t.Fatal(err)
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	if err := recordWorkspaceCheckUsage(db.Session(&gorm.Session{Logger: queries}), []domain.ProxyStatistic{
		{ProxyID: proxy.ID, WorkspaceIDs: []uint{a.ID}, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	for _, query := range queries.queries {
		if strings.Contains(strings.ToLower(query), "user_proxies") {
			t.Fatalf("statistics usage queried inventory: %s", query)
		}
	}
	var usage domain.WorkspaceUsagePeriod
	if err := db.First(&usage, "workspace_id = ?", a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if usage.ActiveRoutes != 17 || usage.PeakActiveRoutes != 17 || usage.CheckAttempts != 1 {
		t.Fatalf("check delta changed active capacity: %+v", usage)
	}
}

func TestUsageMonthCarriesCapacityForChecksAndTraffic(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}); err != nil {
		t.Fatal(err)
	}
	a, b, proxy, _ := seedCheckerEvidenceTest(t, db)
	month, end := workspaceUsageMonth(time.Now().UTC())
	previous := month.AddDate(0, -1, 0)
	for _, id := range []uint{a.ID, b.ID} {
		if err := db.Create(&domain.WorkspaceUsagePeriod{WorkspaceID: id, PeriodStart: previous, PeriodEnd: month, ActiveRoutes: 17, PeakActiveRoutes: 19}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := recordWorkspaceCheckUsage(db, []domain.ProxyStatistic{{ProxyID: proxy.ID, WorkspaceIDs: []uint{a.ID}, CreatedAt: month}}); err != nil {
		t.Fatal(err)
	}
	if err := recordWorkspaceManagedTraffic(db, b.ID, 1, 100, month); err != nil {
		t.Fatal(err)
	}
	var periods []domain.WorkspaceUsagePeriod
	if err := db.Where("period_start = ?", month).Order("workspace_id").Find(&periods).Error; err != nil {
		t.Fatal(err)
	}
	if len(periods) != 2 {
		t.Fatalf("periods = %v", periods)
	}
	for _, period := range periods {
		if period.ActiveRoutes != 17 || period.PeakActiveRoutes != 17 || !period.PeriodEnd.Equal(end) {
			t.Fatalf("month rollover lost capacity: %+v", period)
		}
	}
}
