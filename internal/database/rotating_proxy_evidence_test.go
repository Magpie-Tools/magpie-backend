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

func TestRotatorEvidencePreservesWorkspaceEligibility(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	for i, name := range domain.CheckerProtocols {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	a := domain.Workspace{Name: "Owner A", CheckerGeneration: 1, CheckerHTTPKey: "current-a"}
	b := domain.Workspace{Name: "Owner B", CheckerGeneration: 1, CheckerHTTPKey: "current-b"}
	legacy := domain.Workspace{Name: "Legacy owner"}
	for _, workspace := range []*domain.Workspace{&a, &b, &legacy} {
		if err := db.Create(workspace).Error; err != nil {
			t.Fatal(err)
		}
	}
	for id := uint64(1); id <= 12; id++ {
		if err := db.Create(&domain.Proxy{ID: id, IP: fmt.Sprintf("192.0.2.%d", id), Port: 8080}).Error; err != nil {
			t.Fatal(err)
		}
		for _, workspaceID := range []uint{a.ID, b.ID} {
			managed := domain.ManagedProxy{WorkspaceID: workspaceID, ProxyID: id}
			if id == 1 {
				managed.Username = fmt.Sprintf("owner-%d", workspaceID)
				managed.Password = "workspace-secret"
			}
			if id == 8 {
				managed.State = domain.ManagedProxyStatePaused
			} else if id == 9 {
				managed.State = domain.ManagedProxyStateArchived
			}
			if err := db.Create(&managed).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: legacy.ID, ProxyID: 10}).Error; err != nil {
		t.Fatal(err)
	}
	for _, plan := range []domain.ProxyCheckerPlan{
		{WorkspaceID: a.ID, ProxyID: 3, ProtocolID: 1, ConfigKey: "override-a", Transport: "tcp"},
		{WorkspaceID: a.ID, ProxyID: 12, ProtocolID: 1, ConfigKey: "", Transport: "tcp"},
	} {
		if err := db.Create(&plan).Error; err != nil {
			t.Fatal(err)
		}
	}
	addLatest := func(workspaceID uint, proxyID uint64, key, transport string, alive bool) {
		t.Helper()
		row := domain.ProxyLatestStatistic{WorkspaceID: workspaceID, ProxyID: proxyID,
			ConfigKey: key, ProtocolID: 1, TransportProtocol: transport, Alive: alive,
			StatisticID: proxyID, CheckedAt: time.Now().UTC()}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{1, 3, 7, 8, 9, 12} {
		addLatest(a.ID, id, a.CheckerHTTPKey, "tcp", true)
	}
	addLatest(a.ID, 2, "obsolete", "tcp", true)
	addLatest(a.ID, 2, a.CheckerHTTPKey, "tcp", false)
	addLatest(a.ID, 3, "override-a", "tcp", true)
	addLatest(a.ID, 4, a.CheckerHTTPKey, "quic", true)
	addLatest(b.ID, 1, b.CheckerHTTPKey, "tcp", true)
	addLatest(b.ID, 5, b.CheckerHTTPKey, "tcp", true)
	addLatest(0, 10, "", "quic", true)
	addLatest(a.ID, 11, "", "tcp", true)
	for id, label := range map[uint64]string{1: "good", 3: "neutral", 7: "poor"} {
		if err := db.Create(&domain.ProxyReputation{ProxyID: id, Kind: domain.ProxyReputationKindOverall, Label: label}).Error; err != nil {
			t.Fatal(err)
		}
	}
	judge := domain.Judge{FullString: "http://judge.example.test/"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range []domain.ProxyStatistic{
		{ProxyID: 1, ProtocolID: 1, JudgeID: judge.ID, TransportProtocol: "tcp", Alive: true,
			CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "current-a", Alive: true}, {WorkspaceID: b.ID, ConfigKey: "current-b", Alive: false}}},
		{ProxyID: 3, ProtocolID: 1, JudgeID: judge.ID, TransportProtocol: "tcp", Alive: false,
			CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "override-a", Alive: false}}},
	} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	assertIDs := func(workspaceID uint, labels []string, uptimeType string, percentage *float64, want []uint64) {
		t.Helper()
		ids := make([]uint64, 0)
		if err := buildAliveProxyQuery(db, workspaceID, 1, labels, uptimeType, percentage).
			Distinct("proxies.id").Order("proxies.id").Pluck("proxies.id", &ids).Error; err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ids, want) {
			t.Fatalf("eligible workspace=%d labels=%v uptime=%s IDs=%v want=%v", workspaceID, labels, uptimeType, ids, want)
		}
		count, err := countAliveProxiesForProtocol(db, workspaceID, 1, labels, uptimeType, percentage)
		if err != nil || count != len(want) {
			t.Fatalf("eligible count=%d want=%d error=%v", count, len(want), err)
		}
	}
	assertIDs(a.ID, nil, "", nil, []uint64{1, 3, 7})
	assertIDs(b.ID, nil, "", nil, []uint64{1, 5})
	assertIDs(legacy.ID, nil, "", nil, []uint64{10})
	assertIDs(a.ID, []string{"good"}, "", nil, []uint64{1})
	minimum, maximum := float64(80), float64(20)
	assertIDs(a.ID, nil, uptimeFilterMin, &minimum, []uint64{1})
	assertIDs(a.ID, nil, uptimeFilterMax, &maximum, []uint64{3})
	for _, workspaceID := range []uint{a.ID, b.ID} {
		selected, err := nextAliveProxyForProtocol(db, workspaceID, 1, nil, "", nil, nil)
		if err != nil || selected.ID != 1 || selected.Username != fmt.Sprintf("owner-%d", workspaceID) || selected.Password != "workspace-secret" {
			t.Fatalf("workspace %d credential selection changed: proxy=%+v error=%v", workspaceID, selected, err)
		}
	}
	if err := db.Model(&a).Update("checker_dirty", true).Error; err != nil {
		t.Fatal(err)
	}
	assertIDs(a.ID, nil, "", nil, []uint64{})
	assertIDs(b.ID, nil, "", nil, []uint64{1, 5})
	if err := db.Model(&a).Updates(map[string]any{"checker_dirty": false, "checker_http_key": ""}).Error; err != nil {
		t.Fatal(err)
	}
	assertIDs(a.ID, nil, "", nil, []uint64{3})
}

func TestRotatorEvidenceScopesSharedInventoryPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	routes := 10000
	if value := os.Getenv("MAGPIE_TEST_ROTATOR_ROUTES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1024 {
			t.Fatal("invalid rotator route count", value)
		}
		routes = n
	}
	for i, name := range domain.CheckerProtocols {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	workspaces := make([]domain.Workspace, 8)
	for i := range workspaces {
		workspaces[i] = domain.Workspace{Name: fmt.Sprintf("Shared owner %d", i), CheckerGeneration: 1, CheckerHTTPKey: fmt.Sprintf("current-%d", i)}
		if err := db.Create(&workspaces[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Bulk-load this isolated synthetic schema as pg_restore does, then validate
	// the original foreign keys before measuring either query.
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
SELECT w.id,k.key,'tcp',n,1,NOT k.current OR n%%8=0,n,37,CURRENT_TIMESTAMP
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
	t.Logf("fixture routes=%d shared_owners=8 original_foreign_keys_validated=%d", routes, len(foreignKeys))
	var ignored int64
	query := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return buildAliveProxyQuery(tx, workspaces[0].ID, 1, nil, "", nil).Select("COUNT(DISTINCT proxies.id)").Count(&ignored)
	})
	if strings.Contains(query, "username") || strings.Contains(query, "password") {
		t.Fatal("count query loads credentials")
	}
	if os.Getenv("MAGPIE_TEST_ROTATOR_BASELINE") == "1" {
		baseline := fmt.Sprintf(`SELECT COUNT(DISTINCT proxies.id) FROM proxies
JOIN user_proxies up ON up.proxy_id=proxies.id AND up.workspace_id=%d AND up.state='active'
JOIN (%s) pls ON pls.proxy_id=proxies.id AND pls.checking_workspace_id=up.workspace_id
AND pls.protocol_id=1 AND pls.alive=TRUE AND (pls.transport_protocol='tcp' OR pls.workspace_id=0)`, workspaces[0].ID, currentLatestStatisticsSQL)
		var plan string
		if err := db.Raw("EXPLAIN (FORMAT JSON) " + baseline).Row().Scan(&plan); err != nil {
			t.Fatal(err)
		}
		writeRotatorEvidence(t, "baseline-plan.json", []byte(plan))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		start := time.Now()
		err := db.WithContext(ctx).Raw(baseline).Scan(&ignored).Error
		elapsed := time.Since(start)
		cancel()
		if err == nil && ignored != int64(routes/8) {
			t.Fatalf("baseline eligibility count=%d want=%d", ignored, routes/8)
		}
		t.Logf("baseline routes=%d owners=8 elapsed=%s count=%d error=%v", routes, elapsed, ignored, err)
		result, _ := json.Marshal(map[string]any{"routes": routes, "owners": 8, "elapsed_ms": elapsed.Milliseconds(), "count": ignored, "error": fmt.Sprint(err)})
		writeRotatorEvidence(t, "baseline-latency.json", result)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	count, err := countAliveProxiesForProtocol(db.WithContext(ctx), workspaces[0].ID, 1, nil, "", nil)
	elapsed := time.Since(start)
	if err != nil || count != routes/8 {
		t.Fatalf("scoped count=%d want=%d error=%v", count, routes/8, err)
	}
	var encoded string
	if err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query).Row().Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	writeRotatorEvidence(t, "scoped-plan.json", []byte(encoded))
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
	if visited["proxy_latest_statistics"] > float64(routes) || visited["user_proxies"] > float64(routes) {
		t.Fatalf("count expanded unrelated/shared evidence: routes=%d visited=%v", routes, visited)
	}
	if elapsed > 10*time.Second {
		t.Fatal("scoped rotator count exceeded bounded query budget", elapsed)
	}
	rotator := domain.RotatingProxy{WorkspaceID: workspaces[0].ID, Name: "Shared inventory", ProtocolID: 1, ListenPort: 40123}
	if err := db.Create(&rotator).Error; err != nil {
		t.Fatal(err)
	}
	listed, err := ListRotatingProxies(workspaces[0].ID)
	if err != nil || len(listed) != 1 || listed[0].AliveProxyCount != routes/8 {
		t.Fatalf("shared inventory rotator listing: rows=%v error=%v", listed, err)
	}
	t.Logf("scoped routes=%d owners=8 elapsed=%s SQL_execution_ms=%.3f visited=%v", routes, elapsed, explain[0].ExecutionTime, visited)
	result, _ := json.Marshal(map[string]any{"routes": routes, "owners": 8, "elapsed_ms": elapsed.Milliseconds(), "execution_ms": explain[0].ExecutionTime, "count": count, "rows_visited": visited})
	writeRotatorEvidence(t, "scoped-latency.json", result)
}

func writeRotatorEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	directory := os.Getenv("MAGPIE_TEST_ROTATOR_EVIDENCE_DIR")
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
