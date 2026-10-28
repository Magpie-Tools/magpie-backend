package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type dashboardBoundarySQLCapture struct {
	logger.Interface
	queries []string
}

func (capture *dashboardBoundarySQLCapture) Trace(_ context.Context, _ time.Time, statement func() (string, int64), _ error) {
	query, _ := statement()
	capture.queries = append(capture.queries, query)
}

func TestDashboardDailyBoundaryCountsPreserveOwnershipAndCutoffs(t *testing.T) {
	db := openDashboardBoundaryTestDB(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	today := startOfUTCDay(now)
	cutoff := now.AddDate(0, 0, -7)
	nextDay := startOfUTCDay(cutoff).Add(24 * time.Hour)
	timestamps := []time.Time{
		cutoff.Add(-time.Microsecond), cutoff, nextDay.Add(-time.Microsecond),
		nextDay, nextDay.Add(2 * time.Hour), today.Add(-time.Microsecond),
		today, today.Add(2 * time.Hour), today.Add(24 * time.Hour),
	}
	for proxyID := 1; proxyID <= 5; proxyID++ {
		days := make(map[time.Time]int64)
		for _, checkedAt := range timestamps {
			if err := db.Exec("INSERT INTO proxy_statistics (proxy_id,created_at) VALUES (?,?)", proxyID, checkedAt).Error; err != nil {
				t.Fatal(err)
			}
			days[startOfUTCDay(checkedAt)]++
		}
		for day, count := range days {
			if err := db.Exec("INSERT INTO proxy_daily_checks (proxy_id,day,checks_count) VALUES (?,?,?)", proxyID, day, count).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	for index, state := range []string{domain.ManagedProxyStateActive, domain.ManagedProxyStatePaused, domain.ManagedProxyStateArchived} {
		if err := db.Exec("INSERT INTO user_proxies (workspace_id,proxy_id,state) VALUES (2,?,?)", index+1, state).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Route four moved to another workspace; route five has no owner. Shared
	// route one must count in each current owner, independently of lifecycle.
	if err := db.Exec("INSERT INTO user_proxies (workspace_id,proxy_id,state) VALUES (2,4,'active'),(3,4,'active'),(3,1,'active')").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DELETE FROM user_proxies WHERE workspace_id=2 AND proxy_id=4").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO proxy_check_totals (proxy_id,checks_count,compacted_before) VALUES (1,17,?)", today.AddDate(0, 0, -30)).Error; err != nil {
		t.Fatal(err)
	}

	for _, timezone := range []struct {
		name string
		zone *time.Location
	}{{"UTC", time.UTC}, {"offset", time.FixedZone("offset", 9*3600+1800)}} {
		t.Run(timezone.name, func(t *testing.T) {
			for _, check := range []struct {
				workspaceID uint
				total, week int64
			}{{2, 44, 24}, {3, 35, 16}, {99, 0, 0}} {
				capture := &dashboardBoundarySQLCapture{Interface: logger.Default.LogMode(logger.Silent)}
				got, err := queryDashboardCheckCountsFromDailyWithDB(db.Session(&gorm.Session{Logger: capture}), check.workspaceID, cutoff.In(timezone.zone))
				if err != nil || got.TotalChecks != check.total || got.TotalChecksWeek != check.week {
					t.Fatalf("workspace=%d counts=%+v want=%d/%d error=%v", check.workspaceID, got, check.total, check.week, err)
				}
				if len(capture.queries) != 1 {
					t.Fatalf("dashboard count query budget changed: %v", capture.queries)
				}
				boundarySQL := capture.queries[0]
				guard := regexp.MustCompile(` AND \(ps\.created_at < ["'][^"']+["'] OR ps\.created_at >= ["'][^"']+["']\)`)
				originalSQL := guard.ReplaceAllString(boundarySQL, "")
				if originalSQL == boundarySQL || !strings.Contains(boundarySQL, "proxy_statistics ps") {
					t.Fatalf("production boundary predicate was not captured: %s", boundarySQL)
				}
				var original, bounded dashboardCheckCounts
				if err := db.Raw(originalSQL).Scan(&original).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.Raw(boundarySQL).Scan(&bounded).Error; err != nil || bounded != original {
					t.Fatalf("count totals changed: bounded=%+v original=%+v error=%v", bounded, original, err)
				}
				if timezone.name == "UTC" && check.workspaceID == 2 && db.Dialector.Name() == "postgres" {
					writeDashboardBoundaryEvidence(t, originalSQL, boundarySQL, cutoff, today)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queryDashboardCheckCountsFromDailyWithDB(db.WithContext(ctx).Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}), 2, cutoff); err == nil {
		t.Fatal("canceled dashboard count query succeeded")
	}
}

func openDashboardBoundaryTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	if dsn := os.Getenv("MAGPIE_TEST_POSTGRES_DSN"); dsn != "" {
		admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			t.Fatal(err)
		}
		adminSQL, _ := admin.DB()
		t.Cleanup(func() { _ = adminSQL.Close() })
		schema := fmt.Sprintf("dashboard_boundaries_%d", time.Now().UnixNano())
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
		for _, statement := range []string{
			`CREATE TABLE user_proxies (workspace_id bigint NOT NULL,proxy_id bigint NOT NULL,state text NOT NULL,PRIMARY KEY(workspace_id,proxy_id))`,
			`CREATE TABLE proxy_statistics (id bigserial PRIMARY KEY,proxy_id bigint NOT NULL,created_at timestamptz NOT NULL,alive boolean DEFAULT FALSE)`,
		} {
			if err := db.Exec(statement).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := db.AutoMigrate(&domain.ProxyDailyCheck{}, &domain.ProxyCheckTotal{}); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := openDailyChecksSQLite(t)
	if err := db.Exec("ALTER TABLE user_proxies ADD COLUMN state TEXT NOT NULL DEFAULT 'active'").Error; err != nil {
		t.Fatal(err)
	}
	return db
}

func writeDashboardBoundaryEvidence(t *testing.T, originalSQL, boundedSQL string, cutoff, today time.Time) {
	t.Helper()
	directory := os.Getenv("MAGPIE_TEST_DASHBOARD_BOUNDARY_EVIDENCE_DIR")
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	for name, query := range map[string]string{"old.sql": originalSQL, "bounded.sql": boundedSQL} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(query+";\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	metadata, err := json.MarshalIndent(map[string]any{"cutoff": cutoff, "today_start": today, "workspace_id": 2, "source": "actual queryDashboardCheckCountsFromDailyWithDB SQL; old removes only zero-contribution predicate"}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "generated-sql.json"), append(metadata, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
