package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	testdb "magpie/internal/testkit/db"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Copies of the core's test fixtures, which tests outside internal/database
// cannot reach.

func setupProxyIngestionPostgresTest(t testing.TB) *gorm.DB {
	t.Helper()
	db := testdb.OpenPostgres(t)
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	return db
}

func seedCheckerEvidenceTest(t *testing.T, db *gorm.DB) (domain.Workspace, domain.Workspace, domain.Proxy, domain.Judge) {
	t.Helper()
	return testdb.SeedCheckerEvidence(t, db)
}

func seedCurrentLatestReadTest(t *testing.T, db *gorm.DB) []domain.Workspace {
	t.Helper()
	for i, name := range append(append([]string{}, domain.CheckerProtocols[:]...), "future") {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i, name := range []string{"elite", "anonymous", "transparent"} {
		if err := db.Create(&domain.AnonymityLevel{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := uint(1); i <= 2; i++ {
		if err := db.Create(&domain.Judge{ID: i, FullString: fmt.Sprintf("http://judge-%d.example.test/", i)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	workspaces := []domain.Workspace{
		{Name: "Owner A", CheckerGeneration: 1, CheckerHTTPKey: "http-a", CheckerHTTPSKey: "https-a", CheckerSOCKS5Key: "socks5-a"},
		{Name: "Owner B", CheckerGeneration: 1, CheckerHTTPKey: "http-b"},
		{Name: "Legacy owner"},
		{Name: "Dirty owner", CheckerGeneration: 1, CheckerDirty: true, CheckerHTTPKey: "dirty-http"},
	}
	if err := db.Create(&workspaces).Error; err != nil {
		t.Fatal(err)
	}
	for _, workspace := range workspaces {
		if err := db.Create(&domain.WorkspaceJudge{WorkspaceID: workspace.ID, JudgeID: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for id := uint64(1); id <= 14; id++ {
		if err := db.Create(&domain.Proxy{ID: id, IP: "192.0.2.1", Port: uint16(8000 + id), Country: "DE"}).Error; err != nil {
			t.Fatal(err)
		}
		if id == 14 {
			continue // Latest evidence without ownership must remain invisible.
		}
		for _, workspace := range workspaces {
			state := domain.ManagedProxyStateActive
			if id == 7 {
				state = domain.ManagedProxyStatePaused
			} else if id == 8 {
				state = domain.ManagedProxyStateArchived
			}
			if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: id, State: state}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	a, b, dirty := workspaces[0], workspaces[1], workspaces[3]
	for _, plan := range []domain.ProxyCheckerPlan{
		{WorkspaceID: a.ID, ProxyID: 3, ProtocolID: 1, ConfigKey: "override-http"},
		{WorkspaceID: a.ID, ProxyID: 4, ProtocolID: 2, ConfigKey: "override-https", Transport: "quic"},
		{WorkspaceID: a.ID, ProxyID: 5, ProtocolID: 1, ConfigKey: ""},
		{WorkspaceID: a.ID, ProxyID: 6, ProtocolID: 5, ConfigKey: "future-key"},
		{WorkspaceID: a.ID, ProxyID: 11, ProtocolID: 4, ConfigKey: ""},
	} {
		if err := db.Create(&plan).Error; err != nil {
			t.Fatal(err)
		}
	}
	checked := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	add := func(workspaceID uint, proxyID uint64, protocol int, key, transport string, alive bool, response uint16) {
		t.Helper()
		level := int(proxyID%3 + 1)
		row := domain.ProxyLatestStatistic{WorkspaceID: workspaceID, ProxyID: proxyID, ProtocolID: protocol,
			ConfigKey: key, TransportProtocol: transport, Alive: alive, ResponseTime: response,
			CheckedAt: checked.Add(time.Duration(proxyID) * time.Second), StatisticID: proxyID*10 + uint64(protocol), JudgeID: 1, LevelID: &level}
		if proxyID == 6 {
			row.JudgeID = 2 // The assigned-judge predicate still belongs to its caller.
		}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{1, 5, 7, 8, 14} {
		add(a.ID, id, 1, "http-a", "tcp", true, uint16(id*10))
	}
	add(a.ID, 1, 2, "https-a", "tcp", false, 1)
	add(a.ID, 1, 4, "socks5-a", "tcp", true, 1)
	add(a.ID, 2, 1, "obsolete", "tcp", true, 1)
	add(a.ID, 2, 1, "http-a", "tcp", false, 1)
	add(a.ID, 3, 1, "http-a", "tcp", false, 1)
	add(a.ID, 3, 1, "override-http", "tcp", true, 20)
	add(a.ID, 4, 2, "override-https", "quic", true, 20)
	add(a.ID, 6, 5, "future-key", "quic", true, 0)
	add(a.ID, 10, 1, "http-b", "tcp", true, 1)
	add(a.ID, 11, 4, "socks5-a", "tcp", true, 1)
	add(a.ID, 12, 4, "socks5-a", "quic", true, 1)
	add(a.ID, 13, 1, "", "tcp", true, 1)
	add(b.ID, 1, 1, "http-b", "tcp", false, 1)
	add(b.ID, 2, 1, "http-b", "tcp", true, 35)
	add(dirty.ID, 1, 1, "dirty-http", "tcp", true, 1)
	add(0, 9, 1, "legacy-key", "quic", true, 5)
	add(0, 10, 2, "", "tcp", true, 5)
	if err := db.Exec(`UPDATE proxy_latest_statistics SET response_time=NULL
WHERE workspace_id=? AND ((proxy_id=1 AND protocol_id=4) OR proxy_id=12)`, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ProxyReputation{ProxyID: 4, Kind: "overall", Score: 98, Label: "GOOD"}).Error; err != nil {
		t.Fatal(err)
	}
	return workspaces
}

type proxyPageReadLogger struct {
	logger.Interface
	queries []string
}

func (l *proxyPageReadLogger) Trace(_ context.Context, _ time.Time, sql func() (string, int64), _ error) {
	query, _ := sql()
	l.queries = append(l.queries, query)
}

func explainProxyRead(t *testing.T, db *gorm.DB, name, query string) map[string]any {
	t.Helper()
	var encoded string
	if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query).Row().Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	writeProxyReadEvidence(t, name, []byte(encoded))
	var documents []map[string]any
	if err := json.Unmarshal([]byte(encoded), &documents); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s execution_ms=%v", name, documents[0]["Execution Time"])
	return documents[0]["Plan"].(map[string]any)
}

func visitProxyReadPlan(node map[string]any, visit func(map[string]any)) {
	visit(node)
	if children, ok := node["Plans"].([]any); ok {
		for _, child := range children {
			visitProxyReadPlan(child.(map[string]any), visit)
		}
	}
}

func writeProxyReadEvidence(t testing.TB, name string, data []byte) {
	t.Helper()
	dir := os.Getenv("MAGPIE_TEST_PROXY_READ_EVIDENCE_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
