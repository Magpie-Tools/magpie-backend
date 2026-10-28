package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt in: the representative fixture restores eight million existing latest
// rows before timing updates. Ordinary lock/correctness tests need only a DSN.
func TestStatisticsTransactionCostPopulatedPostgres(t *testing.T) {
	value := os.Getenv("MAGPIE_TEST_STATISTICS_LOCK_ROUTES")
	if value == "" {
		t.Skip("MAGPIE_TEST_STATISTICS_LOCK_ROUTES is not set")
	}
	routes, err := strconv.Atoi(value)
	if err != nil || routes < 20_000 {
		t.Fatal("statistics cost fixture requires at least 20000 routes", value)
	}
	db := setupProxyIngestionPostgresTest(t)
	for _, initialize := range []func(*gorm.DB) error{ensureReadModelSchema, ensureProxyListPerformanceSchema, ensureProxyStatisticsRetentionSchema, ensureProxyQueryIndexSchema} {
		if err := initialize(db); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	judge := domain.Judge{FullString: "http://127.0.0.1:8080/"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := db.Create(&domain.Workspace{Name: fmt.Sprintf("Statistics owner %d", i), CheckerGeneration: 1, CheckerHTTPKey: "current-http"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Fixture bulk restoration follows pg_restore: retain primary/unique keys,
	// then rebuild the original secondary indexes and validate original FKs.
	var foreignKeys []struct{ TableName, Name, Definition string }
	if err := db.Raw(`SELECT c.conrelid::regclass::text AS table_name,c.conname AS name,pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c WHERE c.conrelid IN ('user_proxies'::regclass,'proxy_latest_statistics'::regclass) AND c.contype='f'`).Scan(&foreignKeys).Error; err != nil {
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
WHERE i.indrelid IN ('proxies'::regclass,'user_proxies'::regclass,'proxy_latest_statistics'::regclass)
AND NOT i.indisprimary AND NOT i.indisunique ORDER BY c.relname`).Scan(&indexes).Error; err != nil {
		t.Fatal(err)
	}
	for _, index := range indexes {
		if err := db.Exec("DROP INDEX " + quoteIdentifier(index.Name)).Error; err != nil {
			t.Fatal(err)
		}
	}
	setupStart := time.Now()
	now := time.Now().UTC().Truncate(time.Microsecond)
	seedTime := now.Add(-time.Hour)
	statements := []struct {
		SQL  string
		Args []any
	}{
		{`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type)
SELECT n,'192.0.2.1','192.0.2.1',3128,'N/A','N/A' FROM generate_series(1,?::bigint) n`, []any{routes}},
		{`INSERT INTO proxy_statistic_streams (stream,replay_before,updated_at) VALUES ('checks','',CURRENT_TIMESTAMP)`, nil},
		{`INSERT INTO proxy_statistics (id,alive,attempt,response_time,response_body,transport_protocol,check_timeout,check_retries,check_evidence,protocol_id,proxy_id,judge_id,created_at)
SELECT n,TRUE,0,37,'','tcp',1000,0,e.value,1,n,?,? FROM generate_series(1,?::bigint) n
CROSS JOIN (SELECT jsonb_agg(jsonb_build_object('workspace_id',id,'config_key','current-http','alive',TRUE)) AS value FROM workspaces) e`, []any{judge.ID, seedTime, routes}},
		{`SELECT setval(pg_get_serial_sequence('proxy_statistics','id'),?::bigint)`, []any{routes}},
		{`INSERT INTO proxy_daily_checks (proxy_id,day,checks_count,created_at,updated_at)
SELECT n,?::date,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM generate_series(1,?::bigint) n`, []any{seedTime, routes}},
		{`INSERT INTO proxy_statistic_events (stream,event_id,sort_id,created_at)
SELECT 'checks','1-'||n::text,LPAD('1',20,'0')||'-'||LPAD(n::text,20,'0'),CURRENT_TIMESTAMP FROM generate_series(1,?::bigint) n`, []any{routes}},
		{`INSERT INTO proxy_reputation_refreshes (proxy_id,version,requested_at,lease_until)
SELECT n,1,CURRENT_TIMESTAMP,'0001-01-01'::timestamptz FROM generate_series(1,?::bigint) n`, []any{routes}},
		{`INSERT INTO workspace_usage_periods (workspace_id,period_start,period_end,active_routes,peak_active_routes,check_attempts,managed_requests,managed_bytes,created_at,updated_at)
SELECT id,date_trunc('month',?::timestamptz),date_trunc('month',?::timestamptz)+interval '1 month',?,?,?,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM workspaces`, []any{now, now, routes, routes, routes}},
	}
	for _, statement := range statements {
		t.Logf("fixture: %s", strings.SplitN(statement.SQL, "\n", 2)[0])
		if err := db.Exec(statement.SQL, statement.Args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	for owner := 1; owner <= 8; owner++ {
		t.Logf("fixture: owner %d ownership and preexisting latest rows", owner)
		if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state)
SELECT ?,n,'active' FROM generate_series(1,?::bigint) n`, owner, routes).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,transport_protocol,proxy_id,protocol_id,alive,statistic_id,response_time,attempt,judge_id,checked_at,created_at,updated_at)
SELECT ?,'current-http','tcp',n,1,TRUE,n,37,0,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM generate_series(1,?::bigint) n`, owner, judge.ID, seedTime, routes).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, index := range indexes {
		t.Logf("fixture: restore %s", index.Name)
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
	type fixtureCounts struct{ Routes, Owners, Managed, Latest, History, Daily, Ledger, Reputation int64 }
	var counts fixtureCounts
	if err := db.Raw(`SELECT (SELECT count(*) FROM proxies) AS routes,(SELECT count(*) FROM workspaces) AS owners,
(SELECT count(*) FROM user_proxies) AS managed,(SELECT count(*) FROM proxy_latest_statistics) AS latest,
(SELECT count(*) FROM proxy_statistics) AS history,(SELECT count(*) FROM proxy_daily_checks) AS daily,
(SELECT count(*) FROM proxy_statistic_events) AS ledger,(SELECT count(*) FROM proxy_reputation_refreshes) AS reputation`).Scan(&counts).Error; err != nil {
		t.Fatal(err)
	}
	if counts.Routes != int64(routes) || counts.Owners != 8 || counts.Managed != int64(routes*8) || counts.Latest != int64(routes*8) || counts.History != int64(routes) || counts.Daily != int64(routes) || counts.Ledger != int64(routes) || counts.Reputation != int64(routes) {
		t.Fatal("incomplete representative fixture", counts)
	}
	result := map[string]any{"status": "running", "counts": counts, "validated_foreign_keys": len(foreignKeys), "restored_secondary_indexes": indexes,
		"setup_seconds": time.Since(setupStart).Seconds(), "history_partitioned": true,
		"timing_limits": "Quiet synthetic fixture. SQL tracing overhead is included. Whole-instance WAL deltas include concurrent autovacuum. No application readers, projection worker, network checks or Redis acknowledgement are running."}
	t.Logf("fixture ready in %.3fs: %+v", time.Since(setupStart).Seconds(), counts)
	writeStatisticsOrderCostEvidence(t, result)

	type workerResult struct {
		ElapsedMS, MaxTransactionMS float64
		Committed, Transactions     int
		Error                       string `json:",omitempty"`
	}
	type sample struct {
		Phase, Order          string
		Chunk, Workers, Trial int
		ElapsedMS, WALBytes   float64
		EntryBuildMS          float64
		ExpandedRowsPerWorker int
		Statements            map[string]int
		Results               []workerResult
	}
	var samples []sample
	var committedTotal int64
	stamp := 0
	run := func(order string, chunk, workers, trial int) {
		stamp++
		statsByWorker := make([][]domain.ProxyStatistic, workers)
		var buildMS float64
		for worker := 0; worker < workers; worker++ {
			for i := 0; i < 5000; i++ {
				id := uint64(1 + (i*workers+worker)*routes/(5000*workers))
				stat := domain.ProxyStatistic{ProxyID: id, ProtocolID: 1, JudgeID: judge.ID, Alive: true,
					Attempt: 0, ResponseTime: 37, ResponseBody: strings.Repeat("x", 128), TransportProtocol: "tcp", CheckTimeout: 1000,
					CreatedAt: now.Add(time.Duration(stamp) * time.Second), EventStream: "checks", EventID: fmt.Sprintf("%d-%d", 1000+stamp, i*workers+worker)}
				for owner := uint(1); owner <= 8; owner++ {
					stat.WorkspaceIDs = append(stat.WorkspaceIDs, owner)
					stat.CheckEvidence = append(stat.CheckEvidence, domain.WorkspaceCheckEvidence{WorkspaceID: owner, ConfigKey: "current-http", Alive: true})
				}
				statsByWorker[worker] = append(statsByWorker[worker], stat)
			}
			start := time.Now()
			entries, _ := latestProxyStatusEntries(statsByWorker[worker])
			buildMS += float64(time.Since(start).Microseconds()) / 1000
			if len(entries) != 40_000 {
				t.Fatal("fixture batch did not expand to 40000 unique existing latest rows", len(entries))
			}
		}
		queries := &statisticsCostQueryLogger{Interface: db.Logger, counts: make(map[string]int)}
		measuredDB := db.Session(&gorm.Session{Logger: queries})
		previous := DB
		DB = measuredDB
		defer func() { DB = previous }()
		var beginLSN string
		if err := db.Raw("SELECT pg_current_wal_insert_lsn()::text").Scan(&beginLSN).Error; err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		gate := make(chan struct{})
		out := make(chan workerResult, workers)
		for _, stats := range statsByWorker {
			go func() {
				<-gate
				start := time.Now()
				r := workerResult{}
				for offset := 0; offset < len(stats); offset += chunk {
					part := stats[offset:min(offset+chunk, len(stats))]
					txStart := time.Now()
					var err error
					if order == "usage-before-latest" {
						err = insertStatisticsUsageBeforeLatestPrototype(ctx, measuredDB, part)
					} else {
						err = InsertProxyStatistics(ctx, part, CalculateProxyStatisticBatchSize(len(part)))
					}
					r.Transactions++
					r.MaxTransactionMS = max(r.MaxTransactionMS, float64(time.Since(txStart).Microseconds())/1000)
					if err != nil {
						r.Error = err.Error()
						break
					}
					r.Committed += len(part)
				}
				r.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
				out <- r
			}()
		}
		start := time.Now()
		close(gate)
		s := sample{Phase: fmt.Sprintf("%s-chunk%d-workers%d", order, chunk, workers), Order: order, Chunk: chunk, Workers: workers, Trial: trial, EntryBuildMS: buildMS / float64(workers), ExpandedRowsPerWorker: 40_000}
		for worker := 0; worker < workers; worker++ {
			r := <-out
			s.Results = append(s.Results, r)
			committedTotal += int64(r.Committed)
			if order == "usage-at-tail" && chunk == 625 && (r.Error != "" || r.Committed != 5000) {
				t.Errorf("bounded workload failed shared 30s deadline: %+v", r)
			}
		}
		s.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
		s.Statements = queries.counts
		if err := db.Raw("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),?::pg_lsn)", beginLSN).Scan(&s.WALBytes).Error; err != nil {
			t.Fatal(err)
		}
		samples = append(samples, s)
		result["samples"] = samples
		result["committed_new_physical_events"] = committedTotal
		writeStatisticsOrderCostEvidence(t, result)
		t.Logf("%s trial=%d elapsed_ms=%.3f entry_build_ms=%.3f WAL_bytes=%.0f statements=%v workers=%+v", s.Phase, trial, s.ElapsedMS, s.EntryBuildMS, s.WALBytes, s.Statements, s.Results)
	}
	for trial := 0; trial < 3; trial++ {
		run("usage-before-latest", 5000, 1, trial)
		run("usage-at-tail", 5000, 1, trial)
		run("usage-at-tail", 625, 1, trial)
	}
	for _, mode := range []struct {
		Order string
		Chunk int
	}{{"usage-before-latest", 5000}, {"usage-at-tail", 5000}, {"usage-at-tail", 625}} {
		run(mode.Order, mode.Chunk, 4, 0)
	}
	for owner := 1; owner <= 8; owner++ {
		var usage domain.WorkspaceUsagePeriod
		if err := db.First(&usage, "workspace_id=?", owner).Error; err != nil || usage.CheckAttempts != uint64(int64(routes)+committedTotal) || usage.ActiveRoutes != uint64(routes) || usage.PeakActiveRoutes != uint64(routes) {
			t.Fatalf("owner %d lost accounting/capacity: %+v committed=%d err=%v", owner, usage, committedTotal, err)
		}
	}
	for _, query := range []string{"SELECT count(*) FROM proxy_statistics", "SELECT count(*) FROM proxy_statistic_events", "SELECT sum(checks_count) FROM proxy_daily_checks", "SELECT sum(version) FROM proxy_reputation_refreshes"} {
		var total int64
		if err := db.Raw(query).Scan(&total).Error; err != nil || total != int64(routes)+committedTotal {
			t.Fatalf("accounting mismatch query=%s total=%d committed=%d err=%v", query, total, committedTotal, err)
		}
	}
	var finalLatest struct{ Total, Refreshed int64 }
	if err := db.Raw(`SELECT count(*) AS total,count(*) FILTER (WHERE checked_at >= ?) AS refreshed FROM proxy_latest_statistics`, now).Scan(&finalLatest).Error; err != nil || finalLatest.Total != int64(routes*8) || finalLatest.Refreshed != 20_000*8 {
		t.Fatalf("latest writes did not update exactly the existing eight-owner inventory: %+v err=%v", finalLatest, err)
	}
	result["final_latest_counts"] = finalLatest
	result["status"], result["accounting_checked"] = "passed", true
	if t.Failed() {
		result["status"] = "failed"
	}
	writeStatisticsOrderCostEvidence(t, result)
}

// Same atomic effects as the previous order, with the new deterministic latest
// ordering retained so the comparison isolates usage-row lock placement.
func insertStatisticsUsageBeforeLatestPrototype(ctx context.Context, db *gorm.DB, stats []domain.ProxyStatistic) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		accepted, err := acceptProxyStatisticEvents(tx, stats)
		if err != nil || len(accepted) == 0 {
			return err
		}
		if err := tx.CreateInBatches(&accepted, CalculateProxyStatisticBatchSize(len(accepted))).Error; err != nil {
			return err
		}
		if err := incrementProxyDailyChecks(tx, accepted); err != nil {
			return err
		}
		if err := recordWorkspaceCheckUsage(tx, accepted); err != nil {
			return err
		}
		if err := updateProxyStatusCaches(tx, accepted); err != nil {
			return err
		}
		return queueProxyReputationRefresh(tx, collectProxyIDsFromStatistics(accepted))
	})
}

type statisticsCostQueryLogger struct {
	logger.Interface
	mu     sync.Mutex
	counts map[string]int
}

func (l *statisticsCostQueryLogger) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	query, _ := fc()
	key := "other"
	if _, rest, ok := strings.Cut(query, "INSERT INTO "); ok {
		key = "insert " + strings.SplitN(rest, " ", 2)[0]
	} else if strings.HasPrefix(query, "SELECT") {
		key = "select"
	}
	l.mu.Lock()
	l.counts[key]++
	l.mu.Unlock()
}

func writeStatisticsOrderCostEvidence(t *testing.T, value any) {
	t.Helper()
	dir := os.Getenv("MAGPIE_TEST_STATISTICS_ORDER_EVIDENCE_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cost-results.json"), append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
