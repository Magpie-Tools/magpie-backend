package database

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestDashboardRefreshCancelsBlockedQueriesPostgres(t *testing.T) {
	for _, component := range []string{"job", "info", "recent", "fastest"} {
		t.Run(component, func(t *testing.T) {
			application := fmt.Sprintf("dashboard_cancel_%d", time.Now().UnixNano())
			if dsn := os.Getenv("MAGPIE_TEST_POSTGRES_DSN"); dsn != "" {
				t.Setenv("MAGPIE_TEST_POSTGRES_DSN", dsn+" application_name="+application)
			}
			db := setupProxyIngestionPostgresTest(t)
			workspaces := seedCurrentLatestReadTest(t, db)
			workspaceID := workspaces[0].ID
			version := checkerHealthVersion{Generation: 1}
			info := dashboardInfoCacheEntry{version: version, info: dto.DashboardInfo{TotalChecks: 987}}
			recentKey := dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: defaultRecentProxyChecksLimit}
			fastestKey := dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: dashboardFastestAliveLimit}
			recent := dashboardHealthCacheEntry[[]dto.ProxyRecentCheck]{version: version, value: []dto.ProxyRecentCheck{{ID: 999}}}
			fastest := dashboardHealthCacheEntry[[]dto.ProxyFastestAlive]{version: version, value: []dto.ProxyFastestAlive{{ID: 999}}}
			dashboardInfoCache.Store(workspaceID, info)
			dashboardRecentChecksCache.Store(recentKey, recent)
			dashboardFastestAliveCache.Store(fastestKey, fastest)
			t.Cleanup(func() {
				dashboardInfoCache.Delete(workspaceID)
				dashboardRecentChecksCache.Delete(recentKey)
				dashboardFastestAliveCache.Delete(fastestKey)
			})
			blocker := db.Begin()
			if blocker.Error != nil {
				t.Fatal(blocker.Error)
			}
			defer blocker.Rollback()
			if err := blocker.Exec(`LOCK TABLE proxy_latest_statistics IN ACCESS EXCLUSIVE MODE`).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				tx := db.WithContext(ctx)
				var err error
				switch component {
				case "job":
					err = RefreshDashboardCaches(ctx)
				case "info":
					_, err = refreshDashboardInfoCache(tx, workspaceID)
				case "recent":
					_, err = refreshRecentProxyChecksCache(tx, workspaceID, defaultRecentProxyChecksLimit)
				case "fastest":
					_, err = refreshFastestAliveProxiesCache(tx, workspaceID, dashboardFastestAliveLimit)
				}
				finished <- err
			}()
			var waiting int64
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if err := db.Raw(`SELECT count(*) FROM pg_stat_activity
WHERE application_name=? AND state='active' AND wait_event_type='Lock'`, application).Scan(&waiting).Error; err != nil {
					t.Fatal(err)
				}
				if waiting > 0 {
					break
				}
				select {
				case err := <-finished:
					t.Fatalf("refresh returned before reaching the blocked PostgreSQL query: %v", err)
				default:
				}
				time.Sleep(10 * time.Millisecond)
			}
			if waiting == 0 {
				t.Fatal("refresh did not reach the real PostgreSQL lock")
			}
			start := time.Now()
			cancel()
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("canceled refresh returned success")
				}
			case <-time.After(3 * time.Second):
				_ = blocker.Rollback().Error
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
				}
				t.Fatal("context cancellation did not stop the blocked refresh")
			}
			deadline = start.Add(time.Second)
			for {
				if err := db.Raw(`SELECT count(*) FROM pg_stat_activity
WHERE application_name=? AND state='active' AND wait_event_type='Lock'`, application).Scan(&waiting).Error; err != nil {
					t.Fatal(err)
				}
				if waiting == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("canceled PostgreSQL dashboard queries remain after one second: waiting=%d", waiting)
				}
				time.Sleep(10 * time.Millisecond)
			}
			for _, check := range []struct{ actual, want any }{
				{mustLoadDashboardCache(t, &dashboardInfoCache, workspaceID), info},
				{mustLoadDashboardCache(t, &dashboardRecentChecksCache, recentKey), recent},
				{mustLoadDashboardCache(t, &dashboardFastestAliveCache, fastestKey), fastest},
			} {
				if !reflect.DeepEqual(check.actual, check.want) {
					t.Fatalf("canceled refresh replaced a valid cached snapshot: got=%+v want=%+v", check.actual, check.want)
				}
			}
			t.Logf("observed PostgreSQL lock; %s cancellation=%s; cached snapshots preserved", component, time.Since(start))
		})
	}
}

func mustLoadDashboardCache(t *testing.T, cache interface{ Load(any) (any, bool) }, key any) any {
	t.Helper()
	value, ok := cache.Load(key)
	if !ok {
		t.Fatal("cached snapshot was removed", key)
	}
	return value
}

func TestDashboardInfoQueryFailureKeepsPreviousSnapshotPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace := domain.Workspace{Name: "Dashboard failure", CheckerGeneration: 1}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	previous := dashboardInfoCacheEntry{version: checkerHealthVersion{Generation: 1}, info: dto.DashboardInfo{TotalChecks: 777}}
	dashboardInfoCache.Store(workspace.ID, previous)
	t.Cleanup(func() { dashboardInfoCache.Delete(workspace.ID) })
	if err := db.Migrator().DropTable(&domain.WorkspaceProxyFilterIndex{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := refreshDashboardInfoCache(db.WithContext(ctx), workspace.ID); err == nil {
		t.Fatal("failed dashboard component was published as success")
	}
	if got := mustLoadDashboardCache(t, &dashboardInfoCache, workspace.ID); !reflect.DeepEqual(got, previous) {
		t.Fatal("failed dashboard read replaced the previous snapshot", got)
	}
}

func TestDashboardFastestMetadataIsBoundedPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspaces := seedCurrentLatestReadTest(t, db)
	for _, query := range []string{
		`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'198.51.100.1',8080,'DE','residential' FROM generate_series(15,10014) n`,
		`INSERT INTO proxy_reputations (proxy_id,kind,score,label) SELECT n,'overall',50,'neutral' FROM generate_series(15,10014) n`,
		`ANALYZE proxies`, `ANALYZE proxy_reputations`, `ANALYZE proxy_latest_statistics`, `ANALYZE user_proxies`,
	} {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	queryLog := &proxyPageReadLogger{Interface: logger.Default}
	const limit = 3
	t.Cleanup(func() {
		dashboardFastestAliveCache.Delete(dashboardProxyListCacheKey{WorkspaceID: workspaces[0].ID, Limit: limit})
	})
	rows, err := refreshFastestAliveProxiesCache(db.Session(&gorm.Session{Logger: queryLog}), workspaces[0].ID, limit)
	if err != nil || len(rows) != limit {
		t.Fatalf("fastest candidates=%+v error=%v", rows, err)
	}
	var generated string
	for _, query := range queryLog.queries {
		if strings.HasPrefix(query, "WITH candidates AS MATERIALIZED") {
			generated = query
		}
	}
	if generated == "" {
		t.Fatal("production fastest SQL did not materialize the bounded candidates", queryLog.queries)
	}
	plan := explainProxyRead(t, db, "dashboard-fastest-production-plan.json", generated)
	metadataLookups := make(map[string]float64)
	visitProxyReadPlan(plan, func(node map[string]any) {
		relation, _ := node["Relation Name"].(string)
		if relation == "proxies" || relation == "proxy_reputations" {
			if node["Node Type"] == "Seq Scan" {
				t.Fatal("metadata enrichment scanned the unbounded route/reputation table", node)
			}
			metadataLookups[relation] += node["Actual Loops"].(float64)
		}
	})
	for _, relation := range []string{"proxies", "proxy_reputations"} {
		if metadataLookups[relation] == 0 || metadataLookups[relation] > limit {
			t.Fatalf("%s lookups=%.0f exceed selected candidate limit=%d", relation, metadataLookups[relation], limit)
		}
	}
	t.Logf("actual production query limit=%d metadata lookups=%v; unrelated routes/reputations=10000", limit, metadataLookups)
}
