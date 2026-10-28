package database

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type dashboardStartupQueryLog struct {
	logger.Interface
	mu      sync.Mutex
	queries []string
}

func (l *dashboardStartupQueryLog) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	l.mu.Lock()
	l.queries = append(l.queries, sql)
	l.mu.Unlock()
}

func (l *dashboardStartupQueryLog) matching(t *testing.T, fragment string) string {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, query := range l.queries {
		if strings.Contains(query, fragment) {
			return query
		}
	}
	t.Fatal("production dashboard query was not captured", fragment)
	return ""
}

func TestDashboardWholeInventoryOwnershipPlanPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	for _, model := range []any{
		&domain.Protocol{ID: 1, Name: "http"},
		&domain.AnonymityLevel{ID: 1, Name: "elite"},
		&domain.Judge{ID: 1, FullString: "http://judge.example.test/"},
	} {
		if err := db.Create(model).Error; err != nil {
			t.Fatal(err)
		}
	}
	workspaces := make([]domain.Workspace, 8)
	for i := range workspaces {
		workspaces[i] = domain.Workspace{Name: "Shared owner", CheckerGeneration: 1, CheckerHTTPKey: "current-http"}
	}
	if err := db.Create(&workspaces).Error; err != nil {
		t.Fatal(err)
	}
	for _, workspace := range workspaces {
		if err := db.Create(&domain.WorkspaceJudge{WorkspaceID: workspace.ID, JudgeID: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range []string{
		`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'198.51.100.1',8080,'DE','residential' FROM generate_series(1,10000) n`,
		`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT w.id,n,'active' FROM workspaces w CROSS JOIN generate_series(1,10000) n`,
		`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,statistic_id,checked_at,alive,response_time,judge_id,level_id)
SELECT w.id,w.checker_http_key,n,1,n,'2026-10-22 12:00:00+00',true,100,1,1 FROM workspaces w CROSS JOIN generate_series(1,10000) n`,
		`ANALYZE workspaces`, `ANALYZE user_proxies`, `ANALYZE proxy_latest_statistics`, `ANALYZE proxy_checker_plans`,
	} {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	workspaceID := workspaces[0].ID
	queryLog := &dashboardStartupQueryLog{Interface: logger.Default}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db = db.WithContext(ctx).Session(&gorm.Session{Logger: queryLog})
	t.Cleanup(func() {
		dashboardInfoCache.Delete(workspaceID)
		dashboardRecentChecksCache.Delete(dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: defaultRecentProxyChecksLimit})
		dashboardFastestAliveCache.Delete(dashboardProxyListCacheKey{WorkspaceID: workspaceID, Limit: 3})
	})
	info, err := refreshDashboardInfoCache(db, workspaceID)
	if err != nil || len(info.JudgeValidProxies) != 1 || info.JudgeValidProxies[0].EliteProxies != 10000 {
		t.Fatal("whole-inventory judge result changed", info.JudgeValidProxies, err)
	}
	if _, err := refreshRecentProxyChecksCache(db, workspaceID, defaultRecentProxyChecksLimit); err != nil {
		t.Fatal(err)
	}
	fastest, err := refreshFastestAliveProxiesCache(db, workspaceID, 3)
	if err != nil || len(fastest) != 3 || fastest[0].ID != 1 || fastest[1].ID != 2 || fastest[2].ID != 3 {
		t.Fatal("fastest ties changed", fastest, err)
	}

	queries := map[string]string{
		"judge":   queryLog.matching(t, "AS elite_proxies"),
		"recent":  queryLog.matching(t, "c.ip_address"),
		"fastest": queryLog.matching(t, "FROM candidates latest"),
	}
	newInner := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return dashboardLatestStatistics(tx, workspaceID).Select("pls.*").Find(&[]domain.ProxyLatestStatistic{})
	})
	oldInner := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return workspaceLatestStatistics(tx, workspaceID, true).Select("pls.*").Find(&[]domain.ProxyLatestStatistic{})
	})
	newInner = strings.TrimSuffix(strings.TrimPrefix(newInner, "SELECT pls.* FROM ("), ") pls")
	oldInner = strings.TrimSuffix(strings.TrimPrefix(oldInner, "SELECT pls.* FROM ("), ") pls")
	for _, name := range []string{"judge", "fastest"} {
		query := queries[name]
		if !strings.Contains(query, newInner) {
			t.Fatal("generated dashboard query did not contain its production evidence source", name)
		}
		queries["old-"+name] = strings.Replace(query, newInner, oldInner, 1)
		plan := explainProxyRead(t, db, "dashboard-"+name+"-ownership-plan.json", query)
		var ownershipScans int
		visitProxyReadPlan(plan, func(node map[string]any) {
			if node["Relation Name"] != "user_proxies" {
				return
			}
			ownershipScans++
			// One serial scan, or one per parallel worker, is acceptable. The
			// failed query performed one heap/index probe per alive result.
			if loops, _ := node["Actual Loops"].(float64); loops > 4 {
				t.Fatal("whole-inventory dashboard repeated ownership probes", name, node)
			}
		})
		if ownershipScans != 1 {
			t.Fatal("dashboard ownership must be materialized once", name, ownershipScans)
		}
	}
	if out := os.Getenv("MAGPIE_TEST_DASHBOARD_STARTUP_EVIDENCE_DIR"); out != "" {
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, query := range queries {
			if err := os.WriteFile(filepath.Join(out, name+".sql"), []byte(query+";\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		queryLog.mu.Lock()
		encoded, err := json.MarshalIndent(queryLog.queries, "", "  ")
		queryLog.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "all-production-queries.json"), append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("10,000 routes / eight owners; judge and fastest read authoritative ownership once; current health and tie order preserved")
}
