package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func boundaryCountsDB(t *testing.T, dialect string) *gorm.DB {
	t.Helper()
	if dialect == "sqlite" {
		db := openDailyChecksSQLite(t)
		if err := db.Exec("ALTER TABLE user_proxies ADD COLUMN state TEXT").Error; err != nil {
			t.Fatal(err)
		}
		return db
	}
	dsn := strings.TrimSpace(os.Getenv("MAGPIE_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN is not set")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, _ := admin.DB()
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := fmt.Sprintf("dashboard_counts_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	for _, sql := range []string{
		"CREATE TABLE user_proxies (workspace_id BIGINT NOT NULL, proxy_id BIGINT NOT NULL, state TEXT, PRIMARY KEY(workspace_id,proxy_id))",
		"CREATE TABLE proxy_daily_checks (proxy_id BIGINT NOT NULL, day DATE NOT NULL, checks_count BIGINT NOT NULL, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, PRIMARY KEY(proxy_id,day))",
		"CREATE TABLE proxy_check_totals (proxy_id BIGINT PRIMARY KEY, checks_count BIGINT NOT NULL, compacted_before TIMESTAMPTZ NOT NULL)",
		// Nullable results exercise retained legacy data as well as true/false.
		"CREATE TABLE proxy_statistics (id BIGSERIAL PRIMARY KEY, proxy_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, alive BOOLEAN)",
		"CREATE INDEX ON proxy_statistics (created_at,id)",
		"CREATE INDEX ON proxy_statistics (proxy_id,created_at DESC,id DESC)",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestDashboardCheckCountsBoundaryAndOwnershipSemantics(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			db := boundaryCountsDB(t, dialect)
			today := startOfUTCDay(time.Now())
			owners := map[uint]map[uint64]bool{101: {1: true, 2: true, 3: true}, 102: {1: true, 4: true}}
			for _, row := range []struct {
				workspace, proxy uint
				state            string
			}{
				{101, 1, "active"}, {101, 2, "paused"}, {101, 3, "archived"}, {102, 1, "active"}, {102, 4, "active"},
			} {
				if err := db.Exec("INSERT INTO user_proxies(workspace_id,proxy_id,state) VALUES(?,?,?)", row.workspace, row.proxy, row.state).Error; err != nil {
					t.Fatal(err)
				}
			}
			type dailyRow struct {
				proxy uint64
				day   time.Time
				count int64
			}
			var daily []dailyRow
			retained := map[uint64]int64{1: 7, 3: 11, 4: 19}
			type rawRow struct {
				proxy uint64
				at    time.Time
			}
			var raw []rawRow
			for proxy := uint64(1); proxy <= 5; proxy++ {
				for _, age := range []int{12, 7, 2, 0} {
					d := dailyRow{proxy, today.AddDate(0, 0, -age), int64(age+1) * int64(proxy)}
					daily = append(daily, d)
					if err := db.Exec("INSERT INTO proxy_daily_checks(proxy_id,day,checks_count) VALUES(?,?,?)", d.proxy, d.day, d.count).Error; err != nil {
						t.Fatal(err)
					}
				}
				for i, at := range []time.Time{
					today.AddDate(0, 0, -12), today.AddDate(0, 0, -7).Add(12 * time.Hour), today.AddDate(0, 0, -7).Add(13 * time.Hour),
					today.AddDate(0, 0, -7).Add(14 * time.Hour), today.AddDate(0, 0, -6), today.AddDate(0, 0, -2),
					today, today.Add(12 * time.Hour), today.Add(13 * time.Hour), today.Add(26 * time.Hour),
				} {
					raw = append(raw, rawRow{proxy, at})
					var alive any = i%2 == 0
					if i%3 == 0 {
						alive = nil
					}
					if err := db.Exec("INSERT INTO proxy_statistics(proxy_id,created_at,alive) VALUES(?,?,?)", proxy, at, alive).Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			for proxy, count := range retained {
				if err := db.Exec("INSERT INTO proxy_check_totals(proxy_id,checks_count,compacted_before) VALUES(?,?,?)", proxy, count, today.AddDate(0, 0, -15)).Error; err != nil {
					t.Fatal(err)
				}
			}
			verify := func(workspace uint, cutoff time.Time) {
				t.Helper()
				var want dashboardCheckCounts
				cutoff = cutoff.UTC()
				nextDay := startOfUTCDay(cutoff).Add(24 * time.Hour)
				for _, row := range daily {
					if owners[workspace][row.proxy] && row.day.Before(today) {
						want.TotalChecks += row.count
						if !row.day.Before(nextDay) {
							want.TotalChecksWeek += row.count
						}
					}
				}
				for proxy, count := range retained {
					if owners[workspace][proxy] {
						want.TotalChecks += count
					}
				}
				for _, row := range raw {
					if !owners[workspace][row.proxy] || row.at.Before(cutoff) {
						continue
					}
					if !row.at.Before(today) {
						want.TotalChecks++
						want.TotalChecksWeek++
					}
					if row.at.Before(nextDay) {
						want.TotalChecksWeek++
					}
				}
				got, err := queryDashboardCheckCountsFromDailyWithDB(db, workspace, cutoff)
				if err != nil || got != want {
					t.Fatalf("workspace=%d cutoff=%s got=%+v want=%+v err=%v", workspace, cutoff, got, want, err)
				}
			}
			cutoffs := []time.Time{today.AddDate(0, 0, -7), today.AddDate(0, 0, -7).Add(13 * time.Hour), today.Add(12 * time.Hour), today.AddDate(0, 0, 1)}
			for _, cutoff := range cutoffs {
				for _, workspace := range []uint{0, 101, 102, 999} {
					verify(workspace, cutoff)
					verify(workspace, cutoff.In(time.FixedZone("offset", 9*3600)))
				}
			}
			if err := db.Exec("DELETE FROM user_proxies WHERE workspace_id=101 AND proxy_id=1").Error; err != nil {
				t.Fatal(err)
			}
			delete(owners[101], 1)
			if err := db.Exec("INSERT INTO user_proxies(workspace_id,proxy_id,state) VALUES(101,5,'archived')").Error; err != nil {
				t.Fatal(err)
			}
			owners[101][5] = true
			verify(101, cutoffs[1])
			verify(102, cutoffs[1])
		})
	}
}

func TestDashboardCheckCountsErrorsReturnNoPartialCounts(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			db := boundaryCountsDB(t, dialect)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if got, err := queryDashboardCheckCountsFromDailyWithDB(db.WithContext(ctx), 101, time.Now()); err == nil || got != (dashboardCheckCounts{}) {
				t.Fatal("canceled query", got, err)
			}
			if err := db.Exec("INSERT INTO user_proxies(workspace_id,proxy_id) VALUES(101,1)").Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Exec("INSERT INTO proxy_check_totals(proxy_id,checks_count,compacted_before) VALUES(1,123,?)", time.Now().AddDate(0, 0, -15)).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Exec("DROP TABLE proxy_statistics").Error; err != nil {
				t.Fatal(err)
			}
			if got, err := queryDashboardCheckCountsFromDailyWithDB(db, 101, time.Now()); err == nil || got != (dashboardCheckCounts{}) {
				t.Fatal("failed boundary query published retained counters", got, err)
			}
		})
	}
}

func TestDashboardCheckCountsOneOwnershipScanPostgres(t *testing.T) {
	db := boundaryCountsDB(t, "postgres")
	today := startOfUTCDay(time.Now())
	for _, sql := range []string{
		"INSERT INTO user_proxies(workspace_id,proxy_id,state) SELECT w,p,CASE WHEN p%3=0 THEN 'paused' WHEN p%3=1 THEN 'archived' ELSE 'active' END FROM generate_series(101,108) w CROSS JOIN generate_series(1,4096) p",
		"INSERT INTO proxy_check_totals(proxy_id,checks_count,compacted_before) SELECT p,5,CURRENT_TIMESTAMP - INTERVAL '15 days' FROM generate_series(1,4096) p",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec("INSERT INTO proxy_daily_checks(proxy_id,day,checks_count) SELECT p,?,3 FROM generate_series(1,4096) p", today.AddDate(0, 0, -2)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO proxy_statistics(proxy_id,created_at,alive) SELECT p,?,v FROM generate_series(1,4096) p CROSS JOIN (VALUES(TRUE),(FALSE)) a(v)", today).Error; err != nil {
		t.Fatal(err)
	}
	recorder := &demandSQLRecorder{Interface: logger.Default.LogMode(logger.Silent)}
	got, err := queryDashboardCheckCountsFromDailyWithDB(db.Session(&gorm.Session{Logger: recorder}), 101, today.AddDate(0, 0, -7))
	if err != nil || got.TotalChecks != 4096*10 || got.TotalChecksWeek != 4096*5 {
		t.Fatal(got, err)
	}
	queries := recorder.take()
	if len(queries) != 1 {
		t.Fatalf("count read executed %d SQL statements, want one", len(queries))
	}
	var raw string
	if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + queries[0]).Row().Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plans []struct{ Plan map[string]any }
	if err := json.Unmarshal([]byte(raw), &plans); err != nil {
		t.Fatal(err)
	}
	ownershipScans := 0
	var visit func(map[string]any)
	visit = func(node map[string]any) {
		if node["Relation Name"] == "user_proxies" {
			ownershipScans++
		}
		if children, ok := node["Plans"].([]any); ok {
			for _, child := range children {
				visit(child.(map[string]any))
			}
		}
	}
	visit(plans[0].Plan)
	if ownershipScans != 1 {
		t.Fatalf("actual count plan scanned current ownership %d times, want one", ownershipScans)
	}
	t.Logf("owners=8 routes=4096 SQL_commands=1 actual_ownership_scan_nodes=%d", ownershipScans)
}
