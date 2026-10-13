package database

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/domain"
)

func TestOrphanCounterCleanupKeepsPaceWithDeletedRoutes(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	const routesPerPass = 10_000
	for cycle := 0; cycle < 3; cycle++ {
		start := int64(cycle*routesPerPass + 1)
		end := start + routesPerPass - 1
		for _, query := range []string{
			`INSERT INTO proxies (id, host, port, country, estimated_type, created_at)
			 SELECT n, '127.0.0.1', 8080, '', '', CURRENT_TIMESTAMP FROM generate_series(?::bigint, ?::bigint) n`,
			`INSERT INTO proxy_daily_checks (proxy_id, day, checks_count, created_at, updated_at)
			 SELECT n, CURRENT_DATE - d, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			 FROM generate_series(?::bigint, ?::bigint) n CROSS JOIN generate_series(1, 3) d`,
			`INSERT INTO proxy_daily_check_proxy_backfills (proxy_id, backfilled_at)
			 SELECT n, CURRENT_TIMESTAMP FROM generate_series(?::bigint, ?::bigint) n`,
			`INSERT INTO proxy_check_totals (proxy_id, checks_count, compacted_before)
			 SELECT n, 1, CURRENT_TIMESTAMP - INTERVAL '30 days' FROM generate_series(?::bigint, ?::bigint) n`,
		} {
			if err := db.Exec(query, start, end).Error; err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		deleted, err := DeleteOrphanProxies(ctx)
		cancel()
		if err != nil || deleted != routesPerPass {
			t.Fatalf("pass=%d deleted=%d err=%v", cycle+1, deleted, err)
		}
		for _, table := range []string{"proxy_daily_checks", "proxy_daily_check_proxy_backfills", "proxy_check_totals"} {
			var rows int64
			if err := db.Table(table).Count(&rows).Error; err != nil {
				t.Fatal(err)
			}
			t.Logf("cleanup_pass=%d deleted_routes=%d table=%s surviving_orphan_rows=%d", cycle+1, deleted, table, rows)
			if rows != 0 {
				t.Fatalf("pass %d left %d orphan rows in %s", cycle+1, rows, table)
			}
		}
	}
}

func TestOrphanCounterCleanupDistinctBatchesAndResume(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.ProxyDailyCheck{}, &domain.ProxyDailyCheckProxyBackfill{}, &domain.ProxyCheckTotal{}); err != nil {
		t.Fatal(err)
	}
	active := domain.Proxy{ID: 1, IP: "192.0.2.1", Port: 8080}
	if err := db.Create(&active).Error; err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 8; id++ {
		for day := 1; day <= 5; day++ {
			if err := db.Create(&domain.ProxyDailyCheck{ProxyID: id, Day: startOfUTCDay(time.Now()).AddDate(0, 0, -day), ChecksCount: 1}).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Create(&domain.ProxyDailyCheckProxyBackfill{ProxyID: id, BackfilledAt: time.Now()}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ProxyCheckTotal{ProxyID: id, ChecksCount: 100, CompactedBefore: time.Now()}).Error; err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel after one batch. Its two distinct route IDs must remove all ten
	// daily rows, while the next pass must still drain the remaining tables.
	if err := db.Callback().Raw().After("gorm:raw").Register("test:interrupt_orphan_cleanup", func(tx *gorm.DB) {
		if strings.HasPrefix(tx.Statement.SQL.String(), "DELETE FROM proxy_daily_checks ") && tx.Error == nil {
			if tx.RowsAffected != 10 {
				t.Errorf("first batch removed %d rows, want ten for two distinct routes", tx.RowsAffected)
			}
			cancel()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteOrphanProxyCheckCounters(ctx, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted cleanup: %v", err)
	}
	if err := db.Callback().Raw().Remove("test:interrupt_orphan_cleanup"); err != nil {
		t.Fatal(err)
	}
	if err := DeleteOrphanProxyCheckCounters(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"proxy_daily_checks", "proxy_daily_check_proxy_backfills", "proxy_check_totals"} {
		var orphanRows int64
		if err := db.Table(table).Where("proxy_id <> ?", active.ID).Count(&orphanRows).Error; err != nil || orphanRows != 0 {
			t.Fatalf("remaining %s orphan rows=%d err=%v", table, orphanRows, err)
		}
		var activeRows int64
		if err := db.Table(table).Where("proxy_id = ?", active.ID).Count(&activeRows).Error; err != nil || activeRows == 0 {
			t.Fatalf("live route lost %s rows=%d err=%v", table, activeRows, err)
		}
	}
}
