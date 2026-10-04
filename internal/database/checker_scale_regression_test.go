package database

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/config"
	"magpie/internal/domain"
)

type checkerScaleQueryLogger struct {
	logger.Interface
	queries []string
}

func (l *checkerScaleQueryLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	l.queries = append(l.queries, sql)
	l.Interface.Trace(ctx, begin, fc, err)
}

func TestCheckerRefreshAndRecentChecksScalePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	routes := 20000
	if value := os.Getenv("MAGPIE_TEST_CHECKER_SCALE_ROUTES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 2 {
			t.Fatal("invalid scale size", value)
		}
		routes = n
	}
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	common := seedCheckerTag(t, db, a.ID, "Common checker")
	special := seedCheckerTag(t, db, a.ID, "Special checker")
	commonTimeout, specialTimeout := uint16(2000), uint16(3000)
	a.CheckerConfig.Rules = []dto.TagCheckerRule{{TagID: special.ID, Mode: "add", Protocols: []string{"http"}, Timeout: &specialTimeout}, {TagID: common.ID, Mode: "add", Protocols: []string{"http"}, Timeout: &commonTimeout}}
	judge.SetUp()
	fixture := checkerconfig.Build(a, []domain.ProxyTagAssignment{{WorkspaceID: a.ID, ProxyID: proxy.ID, ProxyTagID: common.ID}}, []domain.JudgeWithRegex{{Judge: &judge, Regex: "accepted"}}, config.GetConfig().Checker.StandardHeader)
	if err := db.Model(&a).Updates(map[string]any{"checker_config": a.CheckerConfig, "checker_generation": 1, "checker_revision": 1, "checker_full_revision": 1, "checker_projected_revision": 1, "checker_http_key": fixture.Default.Keys[0]}).Error; err != nil {
		t.Fatal(err)
	}
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := ensureCheckerSettingsSchema(db); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type) SELECT n,'192.0.2.2','192.0.2.2',8080,'','' FROM generate_series(100,?) n`, []any{98 + routes}},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(100,?) n`, []any{a.ID, 98 + routes}},
		// Refresh planner estimates before the foreign-key checks for a million
		// assignments choose a cached plan based on the original single row.
		{`ANALYZE user_proxies`, nil},
		{`INSERT INTO proxy_tag_assignments (workspace_id,proxy_id,proxy_tag_id) SELECT workspace_id,proxy_id,? FROM user_proxies WHERE workspace_id=?`, []any{common.ID, a.ID}},
		{`INSERT INTO proxy_checker_plans (workspace_id,proxy_id,protocol_id,config_key,transport) SELECT workspace_id,proxy_id,1,?,'tcp' FROM user_proxies WHERE workspace_id=?`, []any{fixture.Plan(proxy.ID).Keys[0], a.ID}},
		{`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,response_time,checked_at) SELECT workspace_id,?,proxy_id,1,proxy_id%2=0,proxy_id,50,'2026-10-01'::timestamptz+proxy_id*interval '1 second' FROM user_proxies WHERE workspace_id=?`, []any{fixture.Plan(proxy.ID).Keys[0], a.ID}},
		{`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,ip_address,port,state,alive,latest_check) SELECT workspace_id,proxy_id,'192.0.2.2','192.0.2.2',8080,'active',proxy_id%2=0,'2026-10-01'::timestamptz+proxy_id*interval '1 second' FROM user_proxies WHERE workspace_id=?`, []any{a.ID}},
	} {
		if err := db.Exec(stmt.sql, stmt.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	stopped, stop := context.WithCancel(context.Background())
	stop()
	if err := checkerconfig.Initialize(stopped, nil, nil, func(context.Context) ([]uint, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := checkerconfig.Initialize(ctx, nil, LoadCheckerWorkspace, func(context.Context) ([]uint, error) { return []uint{a.ID}, nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = checkerconfig.Initialize(stopped, nil, nil, func(context.Context) ([]uint, error) { return nil, nil })
	})
	original, _ := checkerconfig.Lookup(a.ID)
	if original.Proxies.Len() != routes {
		t.Fatal("fixture did not load all tagged routes", original.Proxies.Len())
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: queries})
	measure := func(name string, work func()) {
		t.Helper()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		work()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		allocated := after.TotalAlloc - before.TotalAlloc
		t.Logf("%s routes=%d elapsed=%s allocated=%d bytes", name, routes, elapsed, allocated)
		if allocated > 20*1024*1024 {
			t.Fatalf("%s allocated across the workspace: %d bytes", name, allocated)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("%s scanned workspace: %s", name, elapsed)
		}
	}
	measure("unchanged reconciliation", func() {
		if err := checkerconfig.Refresh(context.Background(), a.ID); err != nil {
			t.Fatal(err)
		}
	})
	for _, sql := range queries.queries {
		if strings.Contains(sql, "proxy_tag_assignments") || strings.Contains(sql, "proxy_checker_plans") || strings.Contains(sql, "checker_proxy_changes") {
			t.Fatal("unchanged reconciliation read route data", sql)
		}
	}
	queries.queries = nil
	measure("one checker membership edit", func() {
		if err := AddProxyTagsToProxies(a.ID, []uint64{proxy.ID}, []uint64{special.ID}); err != nil {
			t.Fatal(err)
		}
	})
	updated, _ := checkerconfig.Lookup(a.ID)
	if updated.Plan(proxy.ID).Settings[0].Timeout != 3000 || original.Plan(proxy.ID).Settings[0].Timeout != 2000 || updated.Plan(100) != original.Plan(100) {
		t.Fatal("incremental snapshot changed an old or unrelated route")
	}
	for _, sql := range queries.queries {
		if strings.HasPrefix(sql, "SELECT") && (strings.Contains(sql, "FROM \"proxy_tag_assignments\"") || strings.Contains(sql, "FROM \"proxy_checker_plans\"")) && !strings.Contains(sql, "proxy_id IN") {
			t.Fatal("membership refresh loaded unscoped routes", sql)
		}
	}
	// Editing one rule affects only that rule's assigned route, even when the
	// workspace's other tag has a million assignments.
	specialTimeout = 3500
	updatedConfigValue := *updated.Value.DefaultCheckerSettings()
	updatedConfigValue.Rules = append([]dto.TagCheckerRule{}, updatedConfigValue.Rules...)
	updatedConfig := &updatedConfigValue
	updatedConfig.Rules[0].Timeout = &specialTimeout
	measure("one checker rule edit", func() {
		if err := UpdateWorkspaceSettings(a.ID, 1, dto.UserSettings{ProvidedFields: map[string]bool{"checker_settings": true}, CheckerSettings: updatedConfig}); err != nil {
			t.Fatal(err)
		}
		if err := checkerconfig.Refresh(context.Background(), a.ID); err != nil {
			t.Fatal(err)
		}
	})
	current, _ := checkerconfig.Lookup(a.ID)
	if current.Plan(proxy.ID).Settings[0].Timeout != 3500 || current.Plan(100) != original.Plan(100) {
		t.Fatal("rule edit changed unrelated routes")
	}
	measure("remove one tagged membership", func() {
		deleted, _, err := DeleteActiveManagedProxy(a.ID, proxy.ID)
		if err != nil || !deleted {
			t.Fatal("deletion failed", deleted, err)
		}
	})
	removed, _ := checkerconfig.Lookup(a.ID)
	if removed.Proxies.Len() != routes-1 || removed.Plan(proxy.ID) != removed.Default || current.Plan(proxy.ID).Settings[0].Timeout != 3500 {
		t.Fatal("membership removal failed or changed a preceding snapshot")
	}
	// Run the actual dashboard query and assert every evidence lookup executes
	// only for the limited candidates, rather than every workspace proxy.
	queries.queries = nil
	start := time.Now()
	recent := RefreshRecentProxyChecksCache(a.ID, 10)
	elapsed := time.Since(start)
	if len(recent) != 10 {
		t.Fatal("recent-check query failed", len(recent))
	}
	var recentSQL string
	for _, sql := range queries.queries {
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
	DB = db
}
