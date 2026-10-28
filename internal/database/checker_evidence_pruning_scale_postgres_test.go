package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestCheckerEvidencePrunePageBoundsPopulatedInventoryPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	routes := 10000
	if value := os.Getenv("MAGPIE_TEST_EVIDENCE_PRUNE_ROUTES"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1000 {
			t.Fatal("invalid evidence prune route count", value)
		}
		routes = parsed
	}
	const batch = 50000
	if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := db.Create(&domain.Workspace{Name: fmt.Sprintf("Prune owner %d", i), CheckerGeneration: 1, CheckerHTTPKey: "current"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	var foreignKeys []struct{ TableName, ConstraintName, Definition string }
	if err := db.Raw(`SELECT c.conrelid::regclass::text AS table_name, c.conname AS constraint_name,
pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c
WHERE c.conrelid IN ('user_proxies'::regclass,'proxy_latest_statistics'::regclass) AND c.contype='f'`).Scan(&foreignKeys).Error; err != nil {
		t.Fatal(err)
	}
	for _, key := range foreignKeys {
		if err := db.Exec("ALTER TABLE " + key.TableName + " DROP CONSTRAINT " + quoteIdentifier(key.ConstraintName)).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		fmt.Sprintf(`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'192.0.2.1',8080,'','' FROM generate_series(1,%d) n`, routes),
		`ANALYZE proxies`, `ANALYZE workspaces`,
		fmt.Sprintf(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT w.id,n,'active' FROM workspaces w CROSS JOIN generate_series(1,%d) n`, routes),
		`ANALYZE user_proxies`,
		fmt.Sprintf(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,transport_protocol,proxy_id,protocol_id,alive,statistic_id,checked_at)
SELECT w.id,'current','tcp',n,1,TRUE,n,CURRENT_TIMESTAMP FROM workspaces w CROSS JOIN generate_series(1,%d) n`, routes),
		`ANALYZE`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range foreignKeys {
		if err := db.Exec("ALTER TABLE " + key.TableName + " ADD CONSTRAINT " + quoteIdentifier(key.ConstraintName) + " " + key.Definition + " NOT VALID").Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("ALTER TABLE " + key.TableName + " VALIDATE CONSTRAINT " + quoteIdentifier(key.ConstraintName)).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("fixture routes=%d owners=8 ownership_rows=%d latest_rows=%d valid_original_FKs=%d", routes, routes*8, routes*8, len(foreignKeys))
	var ownershipIndexes []string
	if err := db.Table("pg_indexes").Where("schemaname=current_schema() AND tablename='user_proxies'").Order("indexname").Pluck("indexdef", &ownershipIndexes).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("ownership indexes=%v", ownershipIndexes)
	if path := os.Getenv("MAGPIE_TEST_EVIDENCE_PRUNE_BASELINE_SQL"); path != "" {
		baseline, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var plan string
		if err := db.Raw("EXPLAIN (FORMAT JSON) "+string(baseline), batch).Row().Scan(&plan); err != nil {
			t.Fatal(err)
		}
		writeEvidencePruneArtifact(t, "baseline-plan.json", []byte(plan))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		start := time.Now()
		err = db.WithContext(ctx).Exec(string(baseline), batch).Error
		elapsed := time.Since(start)
		cancel()
		t.Logf("baseline all-current elapsed=%s error=%v", elapsed, err)
		encoded, _ := json.Marshal(map[string]any{"elapsed_ms": elapsed.Milliseconds(), "error": fmt.Sprint(err)})
		writeEvidencePruneArtifact(t, "baseline-latency.json", encoded)
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: queries})
	defer func() { DB = db }()
	var capturedSQL string
	measure := func(name string, expectedDeleted int64) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		analyze := func(query string) string {
			t.Helper()
			// EXPLAIN ANALYZE executes DELETE CTEs. Roll back its repeat so
			// the following production call sees the same obsolete pointers.
			tx := db.WithContext(ctx).Begin()
			var encoded string
			err := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query).Row().Scan(&encoded)
			_ = tx.Rollback().Error
			if err != nil {
				t.Fatal(err)
			}
			return encoded
		}
		var encoded string
		if capturedSQL != "" {
			encoded = analyze(capturedSQL)
		}
		start := time.Now()
		page, err := PruneObsoleteCheckerEvidencePage(ctx, nil, batch)
		elapsed := time.Since(start)
		if err != nil || page.Scanned != int64(min(batch, routes*8)) || page.Deleted != expectedDeleted {
			if len(queries.queries) > 0 {
				query := queries.queries[len(queries.queries)-1]
				writeEvidencePruneArtifact(t, name+"-failed-query.sql", []byte(query))
				var plan string
				if db.Raw("EXPLAIN (FORMAT JSON) "+query).Row().Scan(&plan) == nil {
					writeEvidencePruneArtifact(t, name+"-failed-plan.json", []byte(plan))
				}
			}
			t.Fatalf("%s page=%+v error=%v", name, page, err)
		}
		query := queries.queries[len(queries.queries)-1]
		capturedSQL = query
		writeEvidencePruneArtifact(t, name+"-query.sql", []byte(query))
		if encoded == "" {
			encoded = analyze(query)
		}
		writeEvidencePruneArtifact(t, name+"-plan.json", []byte(encoded))
		var plans []struct {
			Plan          map[string]any
			ExecutionTime float64 `json:"Execution Time"`
		}
		if err := json.Unmarshal([]byte(encoded), &plans); err != nil {
			t.Fatal(err)
		}
		visited := map[string]float64{}
		var visit func(map[string]any)
		visit = func(node map[string]any) {
			// ModifyTable's RETURNING rows repeat its child scan rows; count
			// physical relation access once rather than counting deleted output.
			if table, ok := node["Relation Name"].(string); ok && node["Node Type"] != "ModifyTable" {
				rows, _ := node["Actual Rows"].(float64)
				removed, _ := node["Rows Removed by Filter"].(float64)
				loops, _ := node["Actual Loops"].(float64)
				visited[table] += (rows + removed) * loops
			}
			if children, ok := node["Plans"].([]any); ok {
				for _, child := range children {
					visit(child.(map[string]any))
				}
			}
		}
		visit(plans[0].Plan)
		latestBudget := page.Scanned + 2*expectedDeleted + 2
		if routes*8 < batch*2 {
			// A tiny table contains fewer than two candidate pages, so a
			// sequential scan can be cheaper than its mostly invisible index.
			// Larger fixtures must visit only this page and obsolete point reads.
			latestBudget = int64(routes*8) + 2*expectedDeleted + 2
		}
		if visited["proxy_latest_statistics"] > float64(latestBudget) || visited["user_proxies"] > float64(page.Scanned) {
			t.Fatalf("page expanded unrelated inventory: %v", visited)
		}
		if elapsed > 10*time.Second {
			t.Fatal("bounded prune page exceeded query budget", elapsed)
		}
		t.Logf("%s scanned=%d deleted=%d elapsed=%s plan_ms=%.3f visited=%v", name, page.Scanned, page.Deleted, elapsed, plans[0].ExecutionTime, visited)
		result, _ := json.Marshal(map[string]any{"routes": routes, "owners": 8, "scanned": page.Scanned, "deleted": page.Deleted, "elapsed_ms": elapsed.Milliseconds(), "plan_ms": plans[0].ExecutionTime, "visited": visited})
		writeEvidencePruneArtifact(t, name+"-latency.json", result)
	}
	measure("all-current", 0)
	// An obsolete prefix proves the same bounded candidate scan also deletes
	// pointers, rather than merely optimizing a current-only early exit.
	if err := db.Exec(`UPDATE proxy_latest_statistics SET config_key='!obsolete' WHERE workspace_id=1 AND proxy_id%64=0`).Error; err != nil {
		t.Fatal(err)
	}
	measure("obsolete-prefix", int64(min(batch, routes/64)))
}

func writeEvidencePruneArtifact(t *testing.T, name string, data []byte) {
	t.Helper()
	directory := os.Getenv("MAGPIE_TEST_EVIDENCE_PRUNE_EVIDENCE_DIR")
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}
