package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestReadModelProjectionScopesPopulatedEvidencePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	routes := 10000
	if value := os.Getenv("MAGPIE_TEST_READ_MODEL_ROUTES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1000 {
			t.Fatal("invalid read-model route count", value)
		}
		routes = n
	}
	batch := 32
	if value := os.Getenv("MAGPIE_TEST_READ_MODEL_BATCH"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > min(routes-100, deleteChunkSize-1) {
			t.Fatal("invalid read-model batch size", value)
		}
		batch = n
	}
	// Spread candidates over the inventory instead of measuring one cached page
	// range. Deliberately reverse input order; projection writes sort lock keys.
	ids := make([]uint64, 0, batch+1)
	for i := batch - 1; i >= 0; i-- {
		ids = append(ids, uint64(1+routes*i/batch))
	}
	ids = append(ids, uint64(routes+1))
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	for i, name := range domain.CheckerProtocols {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	workspaces := make([]domain.Workspace, 8)
	for i := range workspaces {
		workspaces[i] = domain.Workspace{Name: fmt.Sprintf("Shared owner %d", i), CheckerGeneration: 1,
			CheckerHTTPKey: fmt.Sprintf("http-%d", i), CheckerHTTPSKey: fmt.Sprintf("https-%d", i)}
		if err := db.Create(&workspaces[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	legacy := domain.Workspace{Name: "Generation-zero owner"}
	if err := db.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	// Load this synthetic inventory like pg_restore: validate its foreign keys
	// after loading, rather than executing millions of row-level FK triggers.
	// The measured queries run only after every original FK is valid again.
	var foreignKeys []struct{ TableName, ConstraintName, Definition string }
	if err := db.Raw(`SELECT c.conrelid::regclass::text AS table_name, c.conname AS constraint_name,
pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c
WHERE c.conrelid IN ('user_proxies'::regclass,'proxy_latest_statistics'::regclass,'proxy_reputations'::regclass) AND c.contype='f'`).Scan(&foreignKeys).Error; err != nil {
		t.Fatal(err)
	}
	quote := func(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
	for _, key := range foreignKeys {
		if err := db.Exec("ALTER TABLE " + key.TableName + " DROP CONSTRAINT " + quote(key.ConstraintName)).Error; err != nil {
			t.Fatal(err)
		}
	}
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO proxies (id,host,port,country,estimated_type)
SELECT n,'192.0.2.1',8080,'N/A','N/A' FROM generate_series(1,?::bigint) n`, []any{routes + 1}},
		// Foreign-key checks cache plans too. Refresh each newly populated parent
		// before a bulk insert can plan its lookups as if the parent were empty.
		{`ANALYZE proxies`, nil},
		{`INSERT INTO proxy_reputations (proxy_id,kind,score,label,calculated_at,updated_at)
SELECT n,'overall',83,'good',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM generate_series(1,?::bigint) n`, []any{routes + 1}},
		{`ANALYZE workspaces`, nil},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state)
SELECT w.id,n,'active' FROM workspaces w CROSS JOIN generate_series(1,?::bigint) n WHERE w.checker_generation=1`, []any{routes}},
		{`ANALYZE user_proxies`, nil},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state) VALUES (?,1,'active'),(?,?,'paused')`, []any{legacy.ID, workspaces[0].ID, routes + 1}},
		{`UPDATE user_proxies SET pause_reason='capacity' WHERE proxy_id=?`, []any{routes + 1}},
		// Populate current and obsolete keys over unrelated routes as well as all
		// candidates. A sparse/empty latest table hides the former global join.
		{`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,response_time,attempt,checked_at)
SELECT w.id,k.key,n,1,CASE WHEN k.current THEN w.id%2=0 ELSE w.id%2<>0 END,n,37,1,
'2026-10-01'::timestamptz+CASE WHEN k.current THEN interval '0 seconds' ELSE interval '1 day' END
FROM workspaces w CROSS JOIN generate_series(1,?::bigint) n
CROSS JOIN LATERAL (VALUES (w.checker_http_key,TRUE),('obsolete',FALSE)) k(key,current)
WHERE w.checker_generation=1 AND (n%4=0 OR n IN ?)`, []any{routes, ids}},
		{`INSERT INTO proxy_checker_plans (workspace_id,proxy_id,protocol_id,config_key,transport) VALUES (?,1,2,'override-quic','quic')`, []any{workspaces[0].ID}},
		{`INSERT INTO proxy_latest_statistics (workspace_id,config_key,transport_protocol,proxy_id,protocol_id,alive,statistic_id,response_time,checked_at)
VALUES (?,'override-quic','quic',1,2,TRUE,2,7,'2026-10-03'),(?,?,'tcp',1,2,FALSE,3,99,'2026-10-04'),
(0,'','',1,4,TRUE,4,13,'2026-10-05')`, []any{workspaces[0].ID, workspaces[0].ID, workspaces[0].CheckerHTTPSKey}},
		{`UPDATE workspaces SET checker_dirty=TRUE WHERE id=?`, []any{workspaces[0].ID}},
		{`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,port,updated_at)
VALUES (?,?,'unchanged.example',8080,'2000-01-01')`, []any{workspaces[0].ID, routes}},
		{`ANALYZE`, nil},
	}
	for _, statement := range statements {
		if err := db.Exec(statement.sql, statement.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range foreignKeys {
		if err := db.Exec("ALTER TABLE " + key.TableName + " ADD CONSTRAINT " + quote(key.ConstraintName) + " " + key.Definition + " NOT VALID").Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("ALTER TABLE " + key.TableName + " VALIDATE CONSTRAINT " + quote(key.ConstraintName)).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("fixture routes=%d shared_owners=8 original_foreign_keys_validated=%d", routes, len(foreignKeys))
	if path := os.Getenv("MAGPIE_TEST_READ_MODEL_BASELINE_SQL"); path != "" {
		baseline, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var plan string
		if err := db.Raw("EXPLAIN (FORMAT JSON) "+string(baseline), ids).Row().Scan(&plan); err != nil {
			t.Fatal(err)
		}
		writeReadModelEvidence(t, "baseline-plan.json", []byte(plan))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		start := time.Now()
		err = db.WithContext(ctx).Exec(string(baseline), ids).Error
		elapsed := time.Since(start)
		cancel()
		t.Logf("baseline routes=%d owners=8 batch=%d elapsed=%s error=%v", routes, batch+1, elapsed, err)
		result, _ := json.Marshal(map[string]any{"routes": routes, "owners": 8, "batch": batch + 1, "elapsed_ms": elapsed.Milliseconds(), "error": fmt.Sprint(err)})
		writeReadModelEvidence(t, "baseline-latency.json", result)
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	measured := db.Session(&gorm.Session{Logger: queries})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	if err := refreshWorkspaceProxyFilterIndexesForProxyIDs(measured.WithContext(ctx), ids); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	var projectionStatements []string
	for _, sql := range queries.queries {
		if strings.Contains(sql, "INSERT INTO user_proxy_filter_indexes") {
			projectionStatements = append(projectionStatements, sql)
		}
	}
	if len(projectionStatements) == 0 {
		t.Fatal("projection statement was not captured")
	}
	var latestVisited, proxyVisited, reputationVisited, executionMS float64
	evidenceBuilds := 0
	for i, query := range projectionStatements {
		var encoded string
		if err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query).Row().Scan(&encoded); err != nil {
			t.Fatal(err)
		}
		writeReadModelEvidence(t, fmt.Sprintf("scoped-plan-%d.json", i), []byte(encoded))
		var explain []struct {
			Plan          map[string]any
			ExecutionTime float64 `json:"Execution Time"`
		}
		if err := json.Unmarshal([]byte(encoded), &explain); err != nil {
			t.Fatal(err)
		}
		executionMS += explain[0].ExecutionTime
		var visit func(map[string]any)
		visit = func(node map[string]any) {
			if node["Subplan Name"] == "CTE evidence" {
				evidenceBuilds++
			}
			rows, _ := node["Actual Rows"].(float64)
			removed, _ := node["Rows Removed by Filter"].(float64)
			loops, _ := node["Actual Loops"].(float64)
			switch node["Relation Name"] {
			case "proxy_latest_statistics":
				latestVisited += (rows + removed) * loops
			case "proxies":
				proxyVisited += (rows + removed) * loops
			case "proxy_reputations":
				reputationVisited += (rows + removed) * loops
			}
			if children, ok := node["Plans"].([]any); ok {
				for _, child := range children {
					visit(child.(map[string]any))
				}
			}
		}
		visit(explain[0].Plan)
	}
	scopePairs := 8*batch + 2 // Eight owners, one legacy owner and one new route.
	if evidenceBuilds != len(projectionStatements) || latestVisited > float64(scopePairs*4+8) || proxyVisited > float64((batch+1)*len(projectionStatements)) || reputationVisited > float64((batch+1)*len(projectionStatements)) {
		t.Fatalf("projection read unrelated data: builds=%d latest=%.0f proxies=%.0f reputation=%.0f scoped_pairs=%d", evidenceBuilds, latestVisited, proxyVisited, reputationVisited, scopePairs)
	}
	if elapsed > 10*time.Second {
		t.Fatal("scoped projection exceeded bounded batch budget", elapsed)
	}
	t.Logf("scoped routes=%d owners=8 batch=%d pairs=%d elapsed=%s latest_rows_visited=%.0f SQL_execution_ms=%.3f", routes, batch+1, scopePairs, elapsed, latestVisited, executionMS)
	result, _ := json.Marshal(map[string]any{"routes": routes, "owners": 8, "batch": batch + 1, "elapsed_ms": elapsed.Milliseconds(), "latest_rows_visited": latestVisited, "execution_ms": executionMS, "proxy_rows_visited": proxyVisited, "reputation_rows_visited": reputationVisited})
	writeReadModelEvidence(t, "scoped-latency.json", result)
	var rows []domain.WorkspaceProxyFilterIndex
	if err := db.Where("proxy_id IN ?", []uint64{1, uint64(routes + 1), uint64(routes)}).Order("workspace_id,proxy_id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		switch {
		case row.ProxyID == uint64(routes):
			if row.Host != "unchanged.example" || row.UpdatedAt.Year() != 2000 {
				t.Fatal("unrelated projection changed", row)
			}
		case row.ProxyID == uint64(routes+1):
			if row.Alive || row.HealthOverall != nil || !row.LatestCheck.IsZero() || row.State != "paused" || row.PauseReason != "capacity" {
				t.Fatal("new route did not remain paused with unknown health", row)
			}
		case row.WorkspaceID == legacy.ID:
			if !row.Alive || row.ResponseTime != 13 || !row.AliveSOCKS5 {
				t.Fatal("generation-zero evidence changed", row)
			}
		case row.WorkspaceID == workspaces[0].ID:
			if !row.Alive || row.ResponseTime != 7 || row.HealthOverall == nil || *row.HealthOverall != 50 || !row.AliveHTTPS || row.AliveHTTP {
				t.Fatal("committed dirty-workspace override/transport evidence changed", row)
			}
		default:
			if row.Alive != (row.WorkspaceID%2 == 0) || row.ResponseTime != 37 {
				t.Fatal("current default key or workspace attribution changed", row)
			}
		}
	}
	if len(rows) != 11 {
		t.Fatal("missing projected or control rows", len(rows))
	}
	var publicEvidence int64
	if err := currentLatestStatistics(db, workspaces[0].ID).Where("pls.proxy_id=1").Count(&publicEvidence).Error; err != nil || publicEvidence != 0 {
		t.Fatal("public readers exposed dirty-workspace health", publicEvidence, err)
	}
}

func writeReadModelEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := os.Getenv("MAGPIE_TEST_READ_MODEL_EVIDENCE_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReadModelProjectionTimeoutRetainsBatchPostgres(t *testing.T) {
	db, keys := setupSourceStatsShutdownTest(t)
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	flushReadModelDirtyProxyIDs(canceled, false)
	readModelDirtyMu.Lock()
	pending := len(readModelDirtyProxyIDs) + len(readModelDirtyRound)
	if readModelPendingBatch != nil {
		pending += len(readModelPendingBatch.proxyIDs)
	}
	readModelDirtyMu.Unlock()
	if pending != 1 {
		t.Fatal("canceled schema lookup lost its dirty route", pending)
	}
	locked := db.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	defer locked.Rollback()
	if err := locked.Exec("SELECT proxy_id FROM user_proxy_filter_indexes WHERE workspace_id=? FOR UPDATE", keys[0].WorkspaceID).Error; err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	flushReadModelDirtyProxyIDs(context.Background(), false)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatal("locked background projection exceeded query timeout", elapsed)
	}
	readModelDirtyMu.Lock()
	pending = len(readModelDirtyProxyIDs) + len(readModelDirtyRound)
	if readModelPendingBatch != nil {
		pending += len(readModelPendingBatch.proxyIDs)
	}
	readModelDirtyMu.Unlock()
	if pending != 1 {
		t.Fatal("timed-out projection lost its dirty route", pending)
	}
	if err := locked.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	StartReadModelRefreshRoutine(ctx)
	assertSourceStatsCounts(t, db, keys, 1, 0)
	readModelDirtyMu.Lock()
	pending = len(readModelDirtyProxyIDs) + len(readModelDirtyRound)
	if readModelPendingBatch != nil {
		pending += len(readModelPendingBatch.proxyIDs)
	}
	readModelDirtyMu.Unlock()
	if pending != 0 {
		t.Fatal("shutdown retry left a dirty route", pending)
	}
}

func TestReadModelShutdownRetriesTimedOutProjectionPostgres(t *testing.T) {
	db, keys := setupSourceStatsShutdownTest(t)
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	locked := db.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	if err := locked.Exec("SELECT proxy_id FROM user_proxy_filter_indexes WHERE workspace_id=? FOR UPDATE", keys[0].WorkspaceID).Error; err != nil {
		_ = locked.Rollback().Error
		t.Fatal(err)
	}
	failed := make(chan struct{}, 8)
	if err := db.Callback().Raw().After("gorm:raw").Register("test:observe_locked_projection", func(tx *gorm.DB) {
		if tx.Error != nil && strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			select {
			case failed <- struct{}{}:
			default:
			}
		}
	}); err != nil {
		_ = locked.Rollback().Error
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		StartReadModelRefreshRoutine(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		_ = locked.Rollback().Error
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("projection drain did not finish after unlocking")
		}
	})
	// Keep the lock across two complete query deadlines. Returning after the
	// first requeued failure would let Runtime.Shutdown incorrectly succeed.
	for i := 0; i < 2; i++ {
		select {
		case <-failed:
		case <-done:
			t.Fatal("shutdown reported success with a timed-out projection")
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown did not retry the timed-out projection")
		}
	}
	select {
	case <-done:
		t.Fatal("shutdown reported success while the projection row remained locked")
	case <-time.After(50 * time.Millisecond):
	}
	if err := locked.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not drain after releasing the row lock")
	}
	assertSourceStatsCounts(t, db, keys, 1, 0)
	readModelDirtyMu.Lock()
	proxyPending := len(readModelDirtyProxyIDs) + len(readModelDirtyRound)
	if readModelPendingBatch != nil {
		proxyPending += len(readModelPendingBatch.proxyIDs)
	}
	readModelDirtyMu.Unlock()
	sourceStatsDirtyMu.Lock()
	sourcePending := len(sourceStatsDirty)
	sourceStatsDirtyMu.Unlock()
	if proxyPending != 0 || sourcePending != 0 {
		t.Fatalf("successful shutdown left proxies=%d source_pairs=%d", proxyPending, sourcePending)
	}
}
