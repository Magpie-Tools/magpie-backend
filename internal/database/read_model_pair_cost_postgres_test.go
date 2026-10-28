package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

// Opt in: unlike the earlier sparse fixture, all eight owners already have
// projection and latest rows, every physical route has reputation, and all 23
// production projection indexes exist before the measured writes.
func TestReadModelPairCostPopulatedPostgres(t *testing.T) {
	value := os.Getenv("MAGPIE_TEST_READ_MODEL_PAIR_ROUTES")
	if value == "" {
		t.Skip("MAGPIE_TEST_READ_MODEL_PAIR_ROUTES is not set")
	}
	routes, err := strconv.Atoi(value)
	if err != nil || routes < 10000 {
		t.Fatal("invalid populated projection route count", value)
	}
	db := setupProxyIngestionPostgresTest(t)
	for _, initialize := range []func(*gorm.DB) error{ensureReadModelSchema, ensureProxyListPerformanceSchema, ensureProxyQueryIndexSchema} {
		if err := initialize(db); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := db.Create(&domain.Workspace{Name: "Populated projection owner", CheckerGeneration: 1, CheckerHTTPKey: "current-http"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	var foreignKeys []struct{ TableName, Name, Definition string }
	if err := db.Raw(`SELECT c.conrelid::regclass::text AS table_name,c.conname AS name,pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c WHERE c.conrelid IN ('user_proxies'::regclass,'proxy_latest_statistics'::regclass,'proxy_reputations'::regclass) AND c.contype='f'`).Scan(&foreignKeys).Error; err != nil {
		t.Fatal(err)
	}
	for _, key := range foreignKeys {
		if err := db.Exec("ALTER TABLE " + key.TableName + " DROP CONSTRAINT " + quoteIdentifier(key.Name)).Error; err != nil {
			t.Fatal(err)
		}
	}
	var indexes []struct{ Name, Definition string }
	if err := db.Raw(`SELECT c.relname AS name,pg_get_indexdef(c.oid) AS definition
FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
WHERE i.indrelid IN ('proxies'::regclass,'user_proxies'::regclass,'proxy_latest_statistics'::regclass,'proxy_reputations'::regclass,'user_proxy_filter_indexes'::regclass)
AND NOT i.indisprimary AND NOT i.indisunique ORDER BY c.relname`).Scan(&indexes).Error; err != nil {
		t.Fatal(err)
	}
	for _, index := range indexes {
		if err := db.Exec("DROP INDEX " + quoteIdentifier(index.Name)).Error; err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	for _, statement := range []struct {
		SQL  string
		Args []any
	}{
		{`INSERT INTO proxies(id,host,ip_address,port,country,estimated_type)
SELECT n,'192.0.2.1','192.0.2.1',3128,'N/A','N/A' FROM generate_series(1,?::bigint)n`, []any{routes}},
		{`INSERT INTO proxy_reputations(proxy_id,kind,score,label,calculated_at,updated_at)
SELECT n,'overall',70,'good',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM generate_series(1,?::bigint)n`, []any{routes}},
	} {
		if err := db.Exec(statement.SQL, statement.Args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	for owner := 1; owner <= 8; owner++ {
		t.Logf("fixture owner %d: ownership, current latest, existing projection", owner)
		for _, sql := range []string{
			`INSERT INTO user_proxies(workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(1,?::bigint)n`,
			`INSERT INTO proxy_latest_statistics(workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,response_time,attempt,checked_at)
SELECT ?,'current-http',n,1,TRUE,n,37,1,'2026-10-01'::timestamptz FROM generate_series(1,?::bigint)n`,
			`INSERT INTO user_proxy_filter_indexes(workspace_id,proxy_id,host,ip_address,port,state,alive,alive_http,latest_check,response_time,reputation_label,reputation_score)
SELECT ?,n,'192.0.2.1','192.0.2.1',3128,'active',FALSE,FALSE,'2026-09-01'::timestamptz,99,'neutral',50 FROM generate_series(1,?::bigint)n`,
		} {
			if err := db.Exec(sql, owner, routes).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, index := range indexes {
		t.Logf("fixture restore index %s", index.Name)
		if err := db.Exec(index.Definition).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range foreignKeys {
		if err := db.Exec("ALTER TABLE " + key.TableName + " ADD CONSTRAINT " + quoteIdentifier(key.Name) + " " + key.Definition + " NOT VALID").Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("ALTER TABLE " + key.TableName + " VALIDATE CONSTRAINT " + quoteIdentifier(key.Name)).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	definitions := proxyFilterIndexDefinitions(t, db)
	if len(definitions) != 23 {
		t.Fatal("fixture is missing final physical projection indexes", len(definitions), definitions)
	}
	t.Logf("fixture routes=%d owners=8 managed/latest/projection=%d reputation=%d validated_FKs=%d projection_indexes=%d setup_seconds=%.3f", routes, routes*8, routes, len(foreignKeys), len(definitions), time.Since(start).Seconds())
	ids := make([]uint64, 5000)
	for i := range ids {
		ids[i] = uint64(1 + routes*i/len(ids))
	}
	before := &checkerScaleQueryLogger{Interface: db.Logger}
	discoveryStart := time.Now()
	pairs, err := loadReadModelProjectionPairs(db.Session(&gorm.Session{Logger: before}), ids)
	if err != nil || len(pairs) != 40000 {
		t.Fatal("candidate fanout did not contain all eight owners", len(pairs), err)
	}
	discoveryMS := float64(time.Since(discoveryStart).Microseconds()) / 1000
	var baseline map[string]any
	if path := os.Getenv("MAGPIE_TEST_READ_MODEL_PAIR_OLD_SQL"); path != "" {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		started := time.Now()
		var encoded string
		err = db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) " + string(sql)).Row().Scan(&encoded)
		cancel()
		baseline = map[string]any{"wall_ms": float64(time.Since(started).Microseconds()) / 1000, "error": fmt.Sprint(err), "physical_routes": 5000, "pairs": 40000}
		if err == nil {
			writeReadModelEvidence(t, "populated-old-physical-plan.json", []byte(encoded))
		}
		t.Logf("old physical-route batch: %v", baseline)
	}
	var samples []map[string]any
	for next := 0; next < len(pairs); next += 5000 {
		prefix := pairs[next:min(next+5000, len(pairs))]
		queries := &checkerScaleQueryLogger{Interface: db.Logger}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		started := time.Now()
		err := refreshWorkspaceProxyFilterPairs(db.Session(&gorm.Session{Logger: queries}).WithContext(ctx), prefix)
		elapsed := time.Since(started)
		cancel()
		if err != nil {
			t.Fatal("bounded changed-row prefix failed", next, elapsed, err)
		}
		var query string
		for _, sql := range queries.queries {
			if strings.Contains(sql, "INSERT INTO user_proxy_filter_indexes") {
				query = sql
			}
		}
		if query == "" {
			t.Fatal("missing projection SQL")
		}
		// A fresh latest time changes an indexed projection value for every
		// measured row. Do not measure only the no-op retry path.
		keys := make([][]any, len(prefix))
		for i, pair := range prefix {
			keys[i] = []any{pair.WorkspaceID, pair.ProxyID}
		}
		if err := db.Exec(`UPDATE proxy_latest_statistics SET checked_at=checked_at+interval '1 second'
WHERE (workspace_id,proxy_id) IN ?`, keys).Error; err != nil {
			t.Fatal(err)
		}
		for trial := 0; trial < 2; trial++ {
			var encoded string
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			started := time.Now()
			err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) " + query).Row().Scan(&encoded)
			wall := time.Since(started)
			cancel()
			if err != nil {
				t.Fatal("measured bounded projection failed", next, trial, err)
			}
			writeReadModelEvidence(t, fmt.Sprintf("populated-prefix-%d-trial-%d-plan.json", next/5000, trial), []byte(encoded))
			var plan []struct {
				Plan          map[string]any
				ExecutionTime float64 `json:"Execution Time"`
			}
			if err := json.Unmarshal([]byte(encoded), &plan); err != nil {
				t.Fatal(err)
			}
			visits := map[string]float64{}
			var visit func(map[string]any)
			visit = func(node map[string]any) {
				if name, ok := node["Relation Name"].(string); ok && node["Node Type"] != "ModifyTable" {
					rows, _ := node["Actual Rows"].(float64)
					removed, _ := node["Rows Removed by Filter"].(float64)
					loops, _ := node["Actual Loops"].(float64)
					visits[name] += (rows + removed) * loops
				}
				if children, ok := node["Plans"].([]any); ok {
					for _, child := range children {
						visit(child.(map[string]any))
					}
				}
			}
			visit(plan[0].Plan)
			for _, name := range []string{"user_proxies", "proxies", "proxy_reputations", "proxy_latest_statistics"} {
				if visits[name] > 5000 {
					t.Fatal("projection visited unrelated candidate data", name, visits)
				}
			}
			root := plan[0].Plan
			filtered, _ := root["Rows Removed by Conflict Filter"].(float64)
			if root["Conflicting Tuples"] != float64(5000) || (trial == 0 && filtered != 0) || (trial == 1 && filtered != 5000) {
				t.Fatal("changed/no-op measurement did not cover all existing pairs", next, trial, root)
			}
			sample := map[string]any{"prefix": next / 5000, "trial": trial, "pairs": len(prefix), "initial_update_ms": float64(elapsed.Microseconds()) / 1000,
				"wall_ms": float64(wall.Microseconds()) / 1000, "sql_ms": plan[0].ExecutionTime, "visits": visits, "root": root}
			samples = append(samples, sample)
			t.Logf("prefix=%d trial=%d pairs=5000 initial_ms=%.3f SQL_ms=%.3f WAL_bytes=%v visits=%v", next/5000, trial, sample["initial_update_ms"], plan[0].ExecutionTime, root["WAL Bytes"], visits)
		}
	}
	orphanPlans := measurePopulatedReadModelOrphanPlans(t, db, routes)
	result, _ := json.MarshalIndent(map[string]any{"status": "passed", "routes": routes, "owners": 8, "managed_rows": routes * 8, "latest_rows": routes * 8, "projection_rows": routes * 8,
		"reputation_rows": routes, "validated_foreign_keys": len(foreignKeys), "projection_indexes": definitions,
		"discovered_physical_routes": len(ids), "discovered_pairs": len(pairs), "discovery_ms": discoveryMS, "old_physical_batch": baseline, "samples": samples, "orphan_cleanup_plans": orphanPlans}, "", "  ")
	writeReadModelEvidence(t, "populated-pair-cost-results.json", result)
}

func measurePopulatedReadModelOrphanPlans(t *testing.T, db *gorm.DB, routes int) []map[string]any {
	t.Helper()
	for _, sql := range []string{
		`INSERT INTO scrape_sites(id,url) SELECT n,'https://source-'||n||'.invalid/' FROM generate_series(1,10000)n`,
		`INSERT INTO user_scrape_site(workspace_id,scrape_site_id) SELECT w.id,n FROM workspaces w CROSS JOIN generate_series(1,10000)n`,
		`INSERT INTO user_scrape_source_stats(workspace_id,scrape_site_id,url) SELECT w.id,n,'https://source-'||n||'.invalid/' FROM workspaces w CROSS JOIN generate_series(1,10000)n`,
		`ANALYZE user_scrape_site`, `ANALYZE user_scrape_source_stats`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	var results []map[string]any
	for _, cfg := range []struct {
		derived, ownership, key string
		budget, inventory       int
	}{
		{"user_proxy_filter_indexes", "user_proxies", "proxy_id", 5000, routes},
		{"user_scrape_source_stats", "user_scrape_site", "scrape_site_id", 100, 10000},
	} {
		for page, after := range []readModelRepairKey{{}, {WorkspaceID: 4, RowID: uint64(cfg.inventory / 2)}, {WorkspaceID: 8, RowID: uint64(cfg.inventory - cfg.budget)}} {
			queries := &checkerScaleQueryLogger{Interface: db.Logger}
			var keys []readModelRepairKey
			if err := db.Session(&gorm.Session{Logger: queries}).Table(cfg.derived).Select("workspace_id, "+cfg.key+" AS row_id").
				Where("(workspace_id, "+cfg.key+") > (?, ?)", after.WorkspaceID, after.RowID).
				Order("workspace_id, " + cfg.key).Limit(cfg.budget).Scan(&keys).Error; err != nil {
				t.Fatal(err)
			}
			if len(keys) != cfg.budget {
				t.Fatal("incomplete cleanup candidate page", cfg.derived, page, len(keys))
			}
			selectSQL := queries.queries[len(queries.queries)-1]
			if err := db.Exec("DELETE FROM "+cfg.ownership+" WHERE workspace_id=? AND "+cfg.key+"=?", keys[0].WorkspaceID, keys[0].RowID).Error; err != nil {
				t.Fatal(err)
			}
			queries.queries = nil
			if err := deleteOrphanReadModelRows(db.Session(&gorm.Session{DryRun: true, Logger: queries}), cfg.derived, cfg.ownership, cfg.key, keys); err != nil {
				t.Fatal(err)
			}
			deleteSQL := queries.queries[len(queries.queries)-1]
			for phase, sql := range map[string]string{"candidates": selectSQL, "delete": deleteSQL} {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				var encoded string
				err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) " + sql).Row().Scan(&encoded)
				cancel()
				if err != nil {
					t.Fatal("bounded orphan plan failed", cfg.derived, page, phase, err)
				}
				name := fmt.Sprintf("orphan-%s-page-%d-%s", cfg.derived, page, phase)
				writeReadModelEvidence(t, name+".sql", []byte(sql))
				writeReadModelEvidence(t, name+"-plan.json", []byte(encoded))
				var plan []struct {
					Plan          map[string]any
					ExecutionTime float64 `json:"Execution Time"`
				}
				if err := json.Unmarshal([]byte(encoded), &plan); err != nil {
					t.Fatal(err)
				}
				visits := map[string]float64{}
				var walk func(map[string]any)
				walk = func(n map[string]any) {
					if relation, ok := n["Relation Name"].(string); ok && n["Node Type"] != "ModifyTable" {
						if n["Node Type"] == "Seq Scan" {
							t.Fatal("orphan cleanup scanned full table", name, relation)
						}
						rows, _ := n["Actual Rows"].(float64)
						removed, _ := n["Rows Removed by Filter"].(float64)
						loops, _ := n["Actual Loops"].(float64)
						visits[relation] += (rows + removed) * loops
					}
					if children, ok := n["Plans"].([]any); ok {
						for _, child := range children {
							walk(child.(map[string]any))
						}
					}
				}
				walk(plan[0].Plan)
				if visits[cfg.ownership] > float64(cfg.budget) || visits[cfg.derived] > float64(2*cfg.budget) {
					t.Fatal("orphan cleanup read unrelated rows", name, visits)
				}
				results = append(results, map[string]any{"table": cfg.derived, "page": page, "phase": phase, "budget": cfg.budget, "sql_ms": plan[0].ExecutionTime, "visits": visits})
				t.Logf("orphan %s page=%d phase=%s budget=%d SQL_ms=%.3f visits=%v", cfg.derived, page, phase, cfg.budget, plan[0].ExecutionTime, visits)
			}
			var count int64
			if err := db.Table(cfg.derived).Where("workspace_id=? AND "+cfg.key+"=?", keys[0].WorkspaceID, keys[0].RowID).Count(&count).Error; err != nil || count != 0 {
				t.Fatal("planned delete left orphan", cfg.derived, page, count, err)
			}
		}
	}
	return results
}
