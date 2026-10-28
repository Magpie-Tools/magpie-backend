package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestSnapshotAliveCountsPreserveCurrentEvidence(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) *gorm.DB
	}{
		{"sqlite", setupRotatingProxyTestDB},
		{"postgres", func(t *testing.T) *gorm.DB { return setupProxyIngestionPostgresTest(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			for i, name := range domain.CheckerProtocols {
				if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
					t.Fatal(err)
				}
			}
			a := domain.Workspace{Name: "Owner A", CheckerGeneration: 1, CheckerHTTPKey: "http-a", CheckerHTTPSKey: "https-a", CheckerSOCKS5Key: "socks5-a"}
			b := domain.Workspace{Name: "Owner B", CheckerGeneration: 1, CheckerHTTPKey: "http-b"}
			legacy := domain.Workspace{Name: "Legacy owner"}
			legacyShared := domain.Workspace{Name: "Second legacy owner"}
			dirty := domain.Workspace{Name: "Pending settings", CheckerGeneration: 1, CheckerDirty: true, CheckerHTTPKey: "dirty-http"}
			empty := domain.Workspace{Name: "No evidence", CheckerGeneration: 1, CheckerHTTPKey: "empty-http"}
			for _, workspace := range []*domain.Workspace{&a, &b, &legacy, &legacyShared, &dirty, &empty} {
				if err := db.Create(workspace).Error; err != nil {
					t.Fatal(err)
				}
			}
			for id := uint64(1); id <= 18; id++ {
				if err := db.Create(&domain.Proxy{ID: id, IP: fmt.Sprintf("192.0.2.%d", id), Port: 8080}).Error; err != nil {
					t.Fatal(err)
				}
				owners := []uint{a.ID, b.ID}
				if id >= 13 && id <= 16 {
					owners = []uint{legacy.ID, legacyShared.ID}
				} else if id == 17 {
					owners = nil // Orphaned evidence must not count as ownership.
				}
				for _, workspaceID := range owners {
					managed := domain.ManagedProxy{WorkspaceID: workspaceID, ProxyID: id}
					if id == 7 || id == 15 {
						managed.State = domain.ManagedProxyStatePaused
					} else if id == 8 || id == 16 {
						managed.State = domain.ManagedProxyStateArchived
					}
					if err := db.Create(&managed).Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := db.Create(&domain.ManagedProxy{WorkspaceID: dirty.ID, ProxyID: 1}).Error; err != nil {
				t.Fatal(err)
			}
			for _, plan := range []domain.ProxyCheckerPlan{
				{WorkspaceID: a.ID, ProxyID: 3, ProtocolID: 1, ConfigKey: "override-http", Transport: "tcp"},
				{WorkspaceID: a.ID, ProxyID: 4, ProtocolID: 2, ConfigKey: "override-https", Transport: "quic"},
				{WorkspaceID: a.ID, ProxyID: 6, ProtocolID: 3, ConfigKey: "override-socks4", Transport: "tcp"},
				{WorkspaceID: a.ID, ProxyID: 10, ProtocolID: 1, ConfigKey: "", Transport: "tcp"},
			} {
				if err := db.Create(&plan).Error; err != nil {
					t.Fatal(err)
				}
			}
			add := func(workspaceID uint, proxyID uint64, protocolID int, key, transport string, alive bool) {
				t.Helper()
				if err := db.Create(&domain.ProxyLatestStatistic{WorkspaceID: workspaceID, ProxyID: proxyID,
					ProtocolID: protocolID, ConfigKey: key, TransportProtocol: transport,
					Alive: alive, StatisticID: proxyID, CheckedAt: time.Now().UTC()}).Error; err != nil {
					t.Fatal(err)
				}
			}
			for _, id := range []uint64{1, 7, 8, 10, 17, 18} {
				add(a.ID, id, 1, a.CheckerHTTPKey, "tcp", true)
			}
			add(a.ID, 1, 2, a.CheckerHTTPSKey, "tcp", false)
			add(a.ID, 2, 1, "obsolete", "tcp", true)
			add(a.ID, 2, 1, a.CheckerHTTPKey, "tcp", false)
			add(a.ID, 3, 1, a.CheckerHTTPKey, "tcp", false)
			add(a.ID, 3, 1, "override-http", "tcp", true)
			add(a.ID, 4, 2, "override-https", "quic", true)
			add(a.ID, 5, 1, "", "tcp", true)
			add(a.ID, 6, 3, "override-socks4", "tcp", true)
			add(a.ID, 11, 1, b.CheckerHTTPKey, "tcp", true)
			add(a.ID, 18, 4, a.CheckerSOCKS5Key, "quic", true)
			add(b.ID, 1, 1, b.CheckerHTTPKey, "tcp", false)
			add(b.ID, 3, 1, b.CheckerHTTPKey, "tcp", true)
			add(b.ID, 12, 1, b.CheckerHTTPKey, "tcp", true)
			add(dirty.ID, 1, 1, dirty.CheckerHTTPKey, "tcp", true)
			add(0, 13, 1, "", "tcp", true)
			add(0, 14, 1, "", "tcp", false)
			for _, id := range []uint64{9, 14, 15, 16} {
				if err := db.Create(&domain.ProxyOverallStatus{ProxyID: id, OverallAlive: true}).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Create(&domain.ProxyOverallStatus{ProxyID: 13, OverallAlive: false}).Error; err != nil {
				t.Fatal(err)
			}
			workspaceIDs := []uint{a.ID, b.ID, legacy.ID, legacyShared.ID, dirty.ID, empty.ID, 999}
			want := map[uint]int64{a.ID: 5, b.ID: 2, legacy.ID: 1, legacyShared.ID: 1, dirty.ID: 0, empty.ID: 0, 999: 0}
			assertCounts := func(ids []uint, expected map[uint]int64) {
				t.Helper()
				got, err := aliveProxyCountByWorkspace(db, ids)
				if err != nil || !reflect.DeepEqual(got, expected) {
					t.Fatalf("counts=%v want=%v error=%v", got, expected, err)
				}
				// Keep an independent equivalence check against the unchanged public
				// overall-status view, including its generation-zero compatibility path.
				var reference []struct {
					WorkspaceID uint
					AliveCount  int64
				}
				if len(ids) == 0 {
					return
				}
				if err := db.Raw(`SELECT up.workspace_id, COUNT(DISTINCT up.proxy_id) AS alive_count
FROM user_proxies up JOIN (`+currentOverallStatusesSQL+`) pos
ON pos.workspace_id = up.workspace_id AND pos.proxy_id = up.proxy_id
WHERE up.workspace_id IN ? AND up.state = ? AND pos.overall_alive = TRUE
GROUP BY up.workspace_id`, ids, domain.ManagedProxyStateActive).Scan(&reference).Error; err != nil {
					t.Fatal(err)
				}
				for _, row := range reference {
					if got[row.WorkspaceID] != row.AliveCount {
						t.Fatalf("workspace %d count=%d public-view count=%d", row.WorkspaceID, got[row.WorkspaceID], row.AliveCount)
					}
				}
			}
			assertCounts(workspaceIDs, want)
			assertCounts([]uint{b.ID, 999}, map[uint]int64{b.ID: 2, 999: 0})
			assertCounts(nil, map[uint]int64{})
			if err := db.Model(&a).Update("checker_dirty", true).Error; err != nil {
				t.Fatal(err)
			}
			want[a.ID] = 0
			assertCounts(workspaceIDs, want)
			if err := db.Model(&a).Updates(map[string]any{"checker_dirty": false, "checker_http_key": ""}).Error; err != nil {
				t.Fatal(err)
			}
			want[a.ID] = 4 // Route 18 retains current SOCKS5 health after HTTP is disabled.
			assertCounts(workspaceIDs, want)
			if err := db.Model(&legacy).Update("checker_dirty", true).Error; err != nil {
				t.Fatal(err)
			}
			want[legacy.ID] = 0
			assertCounts(workspaceIDs, want)
		})
	}
}

func TestSnapshotAliveCountsScopeSharedInventoryPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	routes := 10000
	if value := os.Getenv("MAGPIE_TEST_SNAPSHOT_ROUTES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1024 {
			t.Fatal("invalid snapshot route count", value)
		}
		routes = n
	}
	if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	workspaces := make([]domain.Workspace, 8)
	workspaceIDs := make([]uint, len(workspaces))
	for i := range workspaces {
		workspaces[i] = domain.Workspace{Name: fmt.Sprintf("Snapshot owner %d", i), CheckerGeneration: 1, CheckerHTTPKey: fmt.Sprintf("current-%d", i)}
		if err := db.Create(&workspaces[i]).Error; err != nil {
			t.Fatal(err)
		}
		workspaceIDs[i] = workspaces[i].ID
	}
	// Keep the large synthetic load separate from its measurements: restore and
	// validate every original foreign key before executing either count query.
	var foreignKeys []struct{ TableName, ConstraintName, Definition string }
	if err := db.Raw(`SELECT c.conrelid::regclass::text AS table_name, c.conname AS constraint_name,
pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c
WHERE c.conrelid IN ('user_proxies'::regclass,'proxy_latest_statistics'::regclass) AND c.contype='f'`).Scan(&foreignKeys).Error; err != nil {
		t.Fatal(err)
	}
	quote := func(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
	for _, key := range foreignKeys {
		if err := db.Exec("ALTER TABLE " + key.TableName + " DROP CONSTRAINT " + quote(key.ConstraintName)).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		fmt.Sprintf(`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'192.0.2.1',8080,'','' FROM generate_series(1,%d) n`, routes),
		`ANALYZE proxies`,
		`ANALYZE workspaces`,
		fmt.Sprintf(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT w.id,n,'active' FROM workspaces w CROSS JOIN generate_series(1,%d) n`, routes),
		`ANALYZE user_proxies`,
		fmt.Sprintf(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,transport_protocol,proxy_id,protocol_id,alive,statistic_id,response_time,checked_at)
SELECT w.id,k.key,'quic',n,1,NOT k.current OR n%%8=0,n,37,CURRENT_TIMESTAMP
FROM workspaces w CROSS JOIN generate_series(1,%d) n
CROSS JOIN LATERAL (VALUES (w.checker_http_key,TRUE),('obsolete',FALSE)) k(key,current) WHERE n%%4=0`, routes),
		`ANALYZE`,
	} {
		if err := db.Exec(statement).Error; err != nil {
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
	t.Logf("fixture routes=%d owners=8 original_foreign_keys_validated=%d", routes, len(foreignKeys))
	if os.Getenv("MAGPIE_TEST_SNAPSHOT_BASELINE") == "1" {
		baseline := `SELECT up.workspace_id, COUNT(DISTINCT up.proxy_id) AS alive_count FROM user_proxies up ` +
			currentOverallStatusJoin(db, "INNER", "up.workspace_id", "up.proxy_id") +
			` WHERE up.workspace_id IN ? AND up.state = 'active' AND pos.overall_alive = TRUE GROUP BY up.workspace_id`
		var plan string
		if err := db.Raw("EXPLAIN (FORMAT JSON) "+baseline, workspaceIDs).Row().Scan(&plan); err != nil {
			t.Fatal(err)
		}
		writeSnapshotEvidence(t, "baseline-plan.json", []byte(plan))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var rows []struct{ WorkspaceID, AliveCount uint }
		start := time.Now()
		err := db.WithContext(ctx).Raw(baseline, workspaceIDs).Scan(&rows).Error
		elapsed := time.Since(start)
		cancel()
		if err == nil {
			if len(rows) != len(workspaceIDs) {
				t.Fatalf("baseline returned %d workspaces, want %d", len(rows), len(workspaceIDs))
			}
			for _, row := range rows {
				if row.AliveCount != uint(routes/8) {
					t.Fatalf("baseline workspace %d count=%d want=%d", row.WorkspaceID, row.AliveCount, routes/8)
				}
			}
		}
		t.Logf("baseline owners=8 elapsed=%s error=%v", elapsed, err)
		encoded, _ := json.Marshal(map[string]any{"routes": routes, "owners": 8, "elapsed_ms": elapsed.Milliseconds(), "error": fmt.Sprint(err)})
		writeSnapshotEvidence(t, "baseline-latency.json", encoded)
	}
	for _, owners := range []int{1, 8} {
		t.Run(fmt.Sprintf("owners-%d", owners), func(t *testing.T) {
			queries := &checkerScaleQueryLogger{Interface: db.Logger}
			measured := db.Session(&gorm.Session{Logger: queries})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			start := time.Now()
			counts, err := aliveProxyCountByWorkspace(measured.WithContext(ctx), workspaceIDs[:owners])
			elapsed := time.Since(start)
			cancel()
			if err != nil || len(counts) != owners {
				t.Fatalf("counts=%v error=%v", counts, err)
			}
			for _, count := range counts {
				if count != int64(routes/8) {
					t.Fatalf("alive count=%d want=%d", count, routes/8)
				}
			}
			if len(queries.queries) != 1 || strings.Contains(queries.queries[0], "LATERAL") {
				t.Fatalf("alive count must use one grouped query: %v", queries.queries)
			}
			ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var encoded string
			if err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + queries.queries[0]).Row().Scan(&encoded); err != nil {
				t.Fatal(err)
			}
			writeSnapshotEvidence(t, fmt.Sprintf("scoped-%d-plan.json", owners), []byte(encoded))
			var explain []struct {
				Plan          map[string]any
				ExecutionTime float64 `json:"Execution Time"`
			}
			if err := json.Unmarshal([]byte(encoded), &explain); err != nil {
				t.Fatal(err)
			}
			visited := map[string]float64{}
			var visit func(map[string]any)
			visit = func(node map[string]any) {
				if table, ok := node["Relation Name"].(string); ok {
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
			visit(explain[0].Plan)
			// At most two pointers per evidenced route, only for requested owners.
			if visited["proxy_latest_statistics"] > float64(routes*owners) || visited["user_proxies"] > float64(routes*owners) {
				t.Fatalf("expanded unrelated ownership/evidence: owners=%d visited=%v", owners, visited)
			}
			if elapsed > 10*time.Second {
				t.Fatal("snapshot count exceeded bounded query budget", elapsed)
			}
			t.Logf("scoped owners=%d elapsed=%s SQL_execution_ms=%.3f visited=%v", owners, elapsed, explain[0].ExecutionTime, visited)
			result, _ := json.Marshal(map[string]any{"routes": routes, "owners": owners, "elapsed_ms": elapsed.Milliseconds(), "execution_ms": explain[0].ExecutionTime, "counts": counts, "rows_visited": visited})
			writeSnapshotEvidence(t, fmt.Sprintf("scoped-%d-latency.json", owners), result)
		})
	}
}

func writeSnapshotEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	directory := os.Getenv("MAGPIE_TEST_SNAPSHOT_EVIDENCE_DIR")
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
