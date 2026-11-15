package dashboard

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/database"
	"magpie/internal/platform/pg"
	"magpie/internal/platform/schema"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

// Moved from the checker scale regression test: the recent-checks query looks
// up evidence only for its limited read-model candidates, never for every
// route of the workspace.
func TestRecentChecksEvidenceLookupsAreBoundedPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	routes := 20000
	if value := os.Getenv("MAGPIE_TEST_CHECKER_SCALE_ROUTES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 2 {
			t.Fatal("invalid scale size", value)
		}
		routes = n
	}
	a, _, _, _ := seedCheckerEvidenceTest(t, db)
	snapshot, err := database.LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil || snapshot == nil {
		t.Fatal("load checker workspace", err)
	}
	key := snapshot.Default.Keys[0]
	if err := schema.EnsureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type) SELECT n,'192.0.2.2','192.0.2.2',8080,'','' FROM generate_series(100,?) n`, []any{98 + routes}},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(100,?) n`, []any{a.ID, 98 + routes}},
		{`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,response_time,checked_at) SELECT workspace_id,?,proxy_id,1,proxy_id%2=0,proxy_id,50,'2026-10-01'::timestamptz+proxy_id*interval '1 second' FROM user_proxies WHERE workspace_id=? AND proxy_id>=100`, []any{key, a.ID}},
		{`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,ip_address,port,state,alive,latest_check) SELECT workspace_id,proxy_id,'192.0.2.2','192.0.2.2',8080,'active',proxy_id%2=0,'2026-10-01'::timestamptz+proxy_id*interval '1 second' FROM user_proxies WHERE workspace_id=? AND proxy_id>=100`, []any{a.ID}},
		{`ANALYZE`, nil},
	} {
		if err := db.Exec(stmt.sql, stmt.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	queries := &testdb.QueryRecorder{Interface: db.Logger}
	pg.DB = db.Session(&gorm.Session{Logger: queries})
	t.Cleanup(func() { pg.DB = db })
	start := time.Now()
	recent := RefreshRecentProxyChecksCache(a.ID, 10)
	elapsed := time.Since(start)
	if len(recent) != 10 {
		t.Fatal("recent-check query failed", len(recent))
	}
	var recentSQL string
	for _, sql := range queries.Queries {
		if strings.Contains(sql, "WITH candidates AS MATERIALIZED") {
			recentSQL = sql
		}
	}
	if recentSQL == "" {
		t.Fatal("dashboard did not use indexed candidates")
	}
	var encoded string
	if err := db.Raw("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + recentSQL).Row().Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var explain []struct {
		Plan          map[string]any
		ExecutionTime float64 `json:"Execution Time"`
	}
	if err := json.Unmarshal([]byte(encoded), &explain); err != nil {
		t.Fatal(err)
	}
	var visit func(map[string]any)
	visit = func(node map[string]any) {
		if node["Relation Name"] == "proxy_latest_statistics" {
			if loops, _ := node["Actual Loops"].(float64); loops > 10 {
				t.Fatalf("dashboard evidence loops=%v before limit", loops)
			}
		}
		if plans, ok := node["Plans"].([]any); ok {
			for _, child := range plans {
				visit(child.(map[string]any))
			}
		}
	}
	visit(explain[0].Plan)
	t.Logf("recent checks routes=%d endpoint=%s SQL_execution_ms=%.3f", routes, elapsed, explain[0].ExecutionTime)
	if elapsed > 5*time.Second {
		t.Fatal("recent dashboard scanned workspace", elapsed)
	}
}
