package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

// Opt in because the release-scale fixture contains eight million preexisting
// projection rows and all production indexes before measuring updates.
func TestProxyFilterIndexUpdateCostPopulatedPostgres(t *testing.T) {
	value := os.Getenv("MAGPIE_TEST_PROXY_FILTER_INDEX_ROUTES")
	if value == "" {
		t.Skip("MAGPIE_TEST_PROXY_FILTER_INDEX_ROUTES is not set")
	}
	routes, err := strconv.Atoi(value)
	if err != nil || routes < 1000 {
		t.Fatal("invalid projection-index route count", value)
	}
	db := setupProxyIngestionPostgresTest(t)
	for _, initialize := range []func(*gorm.DB) error{ensureReadModelSchema, ensureProxyListPerformanceSchema, ensureProxyQueryIndexSchema} {
		if err := initialize(db); err != nil {
			t.Fatal(err)
		}
	}
	newIndexes := []string{"idx_workspace_proxy_filter_page", "idx_workspace_proxy_filter_option_country", "idx_workspace_proxy_filter_option_type", "idx_workspace_proxy_filter_option_anonymity"}
	finalDefinitions := proxyFilterIndexDefinitions(t, db)
	for _, name := range newIndexes {
		if finalDefinitions[name] == "" {
			t.Fatal("final read index is missing", name)
		}
		// Fixture only: start with the original physical set so the final leg
		// also measures the cost of the new page and option indexes.
		if err := db.Exec("DROP INDEX " + quoteIdentifier(name)).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := db.Create(&domain.Workspace{Name: fmt.Sprintf("Index cost owner %d", i), CheckerGeneration: 1, CheckerHTTPKey: "current-http"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	batchSizes := []int{193, min(5000, routes)}
	idsByBatch := make(map[int][]uint64)
	idSet := make(map[uint64]struct{})
	for _, batch := range batchSizes {
		for i := 0; i < batch; i++ {
			id := uint64(1 + routes*i/batch)
			idsByBatch[batch] = append(idsByBatch[batch], id)
			idSet[id] = struct{}{}
		}
	}
	allIDs := make([]uint64, 0, len(idSet))
	for id := range idSet {
		allIDs = append(allIDs, id)
	}
	sort.Slice(allIDs, func(i, j int) bool { return allIDs[i] < allIDs[j] })
	// As in pg_restore, restore the original FK definitions after the synthetic
	// bulk load and validate them before any measured statement.
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
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type)
SELECT n,'192.0.2.1','192.0.2.1',3128,'N/A','N/A' FROM generate_series(1,?::bigint) n`, []any{routes}},
		{`ANALYZE proxies`, nil},
		{`ANALYZE workspaces`, nil},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state)
SELECT w.id,n,'active' FROM workspaces w CROSS JOIN generate_series(1,?::bigint) n`, []any{routes}},
		{`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,response_time,attempt,checked_at)
SELECT w.id,'current-http',n,1,TRUE,n,37,1,'2026-10-01'::timestamptz
FROM workspaces w CROSS JOIN generate_series(1,?::bigint) n WHERE n%4=0 OR n IN ?`, []any{routes, allIDs}},
		{`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,ip_address,port,state,alive,alive_http,latest_check,response_time,reputation_label,reputation_score)
SELECT w.id,n,'192.0.2.1','192.0.2.1',3128,'active',FALSE,FALSE,'2026-09-01'::timestamptz,99,'neutral',50
FROM workspaces w CROSS JOIN generate_series(1,?::bigint) n`, []any{routes}},
		{`ANALYZE`, nil},
	}
	setupStart := time.Now()
	for _, statement := range statements {
		t.Logf("fixture setup: %s", strings.SplitN(statement.sql, "\n", 2)[0])
		if err := db.Exec(statement.sql, statement.args...).Error; err != nil {
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
	createOldProxyFilterIndexes(t, db)
	setupSeconds := time.Since(setupStart).Seconds()
	t.Logf("populated fixture routes=%d owners=8 projection_rows=%d original_FKs_validated=%d setup_seconds=%.3f", routes, routes*8, len(foreignKeys), setupSeconds)

	type sample struct {
		Batch            int     `json:"batch"`
		Rows             int     `json:"existing_rows_updated"`
		ElapsedMS        float64 `json:"elapsed_ms"`
		SQLMS            float64 `json:"sql_ms"`
		WALRecords       float64 `json:"wal_records"`
		WALBytes         float64 `json:"wal_bytes"`
		SharedHitBlocks  float64 `json:"shared_hit_blocks"`
		SharedReadBlocks float64 `json:"shared_read_blocks"`
		SharedDirtied    float64 `json:"shared_dirtied_blocks"`
	}
	measure := func(phase string) []sample {
		var samples []sample
		for _, batch := range batchSizes {
			ids := idsByBatch[batch]
			queries := &checkerScaleQueryLogger{Interface: db.Logger}
			if err := refreshWorkspaceProxyFilterIndexesForProxyIDs(db.Session(&gorm.Session{Logger: queries}), ids); err != nil {
				t.Fatal(err)
			}
			var query string
			for _, sql := range queries.queries {
				if strings.Contains(sql, "INSERT INTO user_proxy_filter_indexes") {
					query = sql
				}
			}
			if query == "" {
				t.Fatal("projection query was not captured")
			}
			for trial := 0; trial < 3; trial++ {
				// Every measurement changes an indexed value, as a fresh checker
				// result does. This preparation is outside the measured upsert.
				if err := db.Exec(`UPDATE proxy_latest_statistics SET checked_at=checked_at+interval '1 second'
WHERE proxy_id IN ?`, ids).Error; err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				start := time.Now()
				var encoded string
				err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) " + query).Row().Scan(&encoded)
				elapsed := time.Since(start)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				var plan []struct {
					Plan          map[string]any
					ExecutionTime float64 `json:"Execution Time"`
				}
				if err := json.Unmarshal([]byte(encoded), &plan); err != nil {
					t.Fatal(err)
				}
				root := plan[0].Plan
				if root["Tuples Inserted"] != float64(0) || root["Conflicting Tuples"] != float64(batch*8) {
					t.Fatal("measurement did not update exactly the preexisting owner rows", root)
				}
				s := sample{Batch: batch, Rows: batch * 8, ElapsedMS: float64(elapsed.Microseconds()) / 1000,
					SQLMS: plan[0].ExecutionTime, WALRecords: root["WAL Records"].(float64), WALBytes: root["WAL Bytes"].(float64),
					SharedHitBlocks: root["Shared Hit Blocks"].(float64), SharedReadBlocks: root["Shared Read Blocks"].(float64),
					SharedDirtied: root["Shared Dirtied Blocks"].(float64)}
				samples = append(samples, s)
				t.Logf("%s batch=%d owner_rows=%d trial=%d SQL_ms=%.3f elapsed_ms=%.3f WAL_records=%.0f WAL_bytes=%.0f shared_hit=%.0f read=%.0f dirtied=%.0f", phase, batch, s.Rows, trial, s.SQLMS, s.ElapsedMS, s.WALRecords, s.WALBytes, s.SharedHitBlocks, s.SharedReadBlocks, s.SharedDirtied)
				writeProxyFilterIndexEvidence(t, fmt.Sprintf("%s-%d-%d-plan.json", phase, batch, trial), []byte(encoded))
			}
		}
		return samples
	}
	beforeDefinitions := proxyFilterIndexDefinitions(t, db)
	var beforeBytes int64
	if err := db.Raw(`SELECT pg_indexes_size('user_proxy_filter_indexes')`).Scan(&beforeBytes).Error; err != nil {
		t.Fatal(err)
	}
	before := measure("original")
	var beforeCleanupBytes int64
	if err := db.Raw(`SELECT pg_indexes_size('user_proxy_filter_indexes')`).Scan(&beforeCleanupBytes).Error; err != nil {
		t.Fatal(err)
	}
	migrationStart := time.Now()
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	migrationMS := float64(time.Since(migrationStart).Microseconds()) / 1000
	afterDefinitions := proxyFilterIndexDefinitions(t, db)
	if len(beforeDefinitions)-len(afterDefinitions) != 13 {
		t.Fatal("migration did not remove exactly thirteen copies", beforeDefinitions, afterDefinitions)
	}
	var afterBytes int64
	if err := db.Raw(`SELECT pg_indexes_size('user_proxy_filter_indexes')`).Scan(&afterBytes).Error; err != nil {
		t.Fatal(err)
	}
	canonicalBase := measure("canonical-base")
	newIndexStart := time.Now()
	if err := ensureProxyListPerformanceSchema(db); err != nil {
		t.Fatal(err)
	}
	newIndexMS := float64(time.Since(newIndexStart).Microseconds()) / 1000
	withNewDefinitions := proxyFilterIndexDefinitions(t, db)
	for _, name := range newIndexes {
		if withNewDefinitions[name] != finalDefinitions[name] {
			t.Fatal("final read index definition changed", name, withNewDefinitions[name], finalDefinitions[name])
		}
	}
	if len(withNewDefinitions)-len(afterDefinitions) != len(newIndexes) {
		t.Fatal("final schema did not add exactly the selected read indexes", withNewDefinitions, afterDefinitions)
	}
	var finalBytes int64
	if err := db.Raw(`SELECT pg_indexes_size('user_proxy_filter_indexes')`).Scan(&finalBytes).Error; err != nil {
		t.Fatal(err)
	}
	after := measure("final")
	for _, batch := range batchSizes {
		var beforeRecords, afterRecords float64
		for _, s := range before {
			if s.Batch == batch {
				beforeRecords += s.WALRecords
			}
		}
		for _, s := range canonicalBase {
			if s.Batch == batch {
				afterRecords += s.WALRecords
			}
		}
		if afterRecords >= beforeRecords {
			t.Fatal("removing physical copies did not reduce measured upsert WAL records", batch, beforeRecords, afterRecords)
		}
		var current int64
		if err := db.Raw(`SELECT count(*) FROM user_proxy_filter_indexes ufi
JOIN proxy_latest_statistics pls ON pls.workspace_id=ufi.workspace_id AND pls.proxy_id=ufi.proxy_id
WHERE ufi.proxy_id IN ? AND ufi.alive AND ufi.alive_http AND ufi.latest_check=pls.checked_at`, idsByBatch[batch]).Scan(&current).Error; err != nil {
			t.Fatal(err)
		}
		if current != int64(batch*8) {
			t.Fatal("updated projection lost current owner evidence", batch, current)
		}
	}
	var counts struct{ Routes, Owners, Managed, Projected, Latest int64 }
	if err := db.Raw(`SELECT (SELECT count(*) FROM proxies) AS routes,(SELECT count(*) FROM workspaces) AS owners,
(SELECT count(*) FROM user_proxies) AS managed,(SELECT count(*) FROM user_proxy_filter_indexes) AS projected,
(SELECT count(*) FROM proxy_latest_statistics) AS latest`).Scan(&counts).Error; err != nil {
		t.Fatal(err)
	}
	if counts.Routes != int64(routes) || counts.Owners != 8 || counts.Managed != int64(routes*8) || counts.Projected != int64(routes*8) {
		t.Fatal("fixture inventory changed", counts)
	}
	result := map[string]any{"routes": routes, "owners": 8, "inventory": counts,
		"validated_foreign_keys": len(foreignKeys), "setup_seconds": setupSeconds, "total_seconds": time.Since(setupStart).Seconds(),
		"original_indexes": beforeDefinitions, "canonical_base_indexes": afterDefinitions, "final_indexes": withNewDefinitions,
		"original_index_bytes": beforeBytes, "canonical_base_index_bytes": afterBytes, "final_index_bytes": finalBytes,
		"removed_copy_bytes": beforeCleanupBytes - afterBytes, "duplicate_cleanup_ms": migrationMS, "new_read_index_creation_ms": newIndexMS,
		"original_samples": before, "canonical_base_samples": canonicalBase, "final_samples": after}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeProxyFilterIndexEvidence(t, "results.json", encoded)
	t.Logf("index count=%d->%d->%d bytes=%d->%d->%d duplicate_cleanup_ms=%.3f new_read_indexes_ms=%.3f", len(beforeDefinitions), len(afterDefinitions), len(withNewDefinitions), beforeBytes, afterBytes, finalBytes, migrationMS, newIndexMS)
}

func writeProxyFilterIndexEvidence(t testing.TB, name string, encoded []byte) {
	t.Helper()
	if path := os.Getenv("MAGPIE_TEST_PROXY_FILTER_INDEX_EVIDENCE_DIR"); path != "" {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, name), encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
