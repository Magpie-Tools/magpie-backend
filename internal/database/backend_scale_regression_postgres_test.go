package database

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func TestWorkspaceCheckUsageInventoryScalePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, _, _ := seedCheckerEvidenceTest(t, db)
	statistics := make([]domain.ProxyStatistic, 1000)
	for i := range statistics {
		statistics[i] = domain.ProxyStatistic{ProxyID: 1, WorkspaceIDs: []uint{a.ID}, CreatedAt: time.Now().UTC()}
	}
	previous := 1
	for _, target := range []int{1000, 100000, 1000000} {
		if err := db.Exec(`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'192.0.2.1',8080,'','' FROM generate_series(?::bigint,?::bigint) n`, previous+1, target).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(?::bigint,?::bigint) n`, a.ID, previous+1, target).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("ANALYZE user_proxies").Error; err != nil {
			t.Fatal(err)
		}
		previous = target
		var samples []time.Duration
		for i := 0; i < 5; i++ {
			start := time.Now()
			err := db.Transaction(func(tx *gorm.DB) error { return recordWorkspaceCheckUsage(tx, statistics) })
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, time.Since(start))
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		var encoded string
		if err := db.Raw(`EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT workspace_id,COUNT(*) FROM user_proxies WHERE workspace_id IN (?) AND state='active' GROUP BY workspace_id`, a.ID).Row().Scan(&encoded); err != nil {
			t.Fatal(err)
		}
		var plan []struct {
			Plan          map[string]any
			ExecutionTime float64 `json:"Execution Time"`
		}
		if err := json.Unmarshal([]byte(encoded), &plan); err != nil {
			t.Fatal(err)
		}
		var scanned float64
		var visit func(map[string]any)
		visit = func(node map[string]any) {
			if node["Relation Name"] == "user_proxies" {
				rows, _ := node["Actual Rows"].(float64)
				loops, _ := node["Actual Loops"].(float64)
				scanned += rows * loops
			}
			if children, ok := node["Plans"].([]any); ok {
				for _, child := range children {
					visit(child.(map[string]any))
				}
			}
		}
		visit(plan[0].Plan)
		t.Logf("active_routes=%d fixed_batch=1000 median_usage_write=%s count_query_ms=%.3f rows_scanned=%.0f", target, samples[2], plan[0].ExecutionTime, scanned)
	}
}

func TestRotatorCountLargePoolPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, _, _ := seedCheckerEvidenceTest(t, db)
	const routes = 65536
	if err := db.Exec(`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'192.0.2.1',8080,'','' FROM generate_series(2,?::bigint) n`, routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(2,?) n`, a.ID, routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,response_time,checked_at) SELECT 0,'',n,1,TRUE,n,50,CURRENT_TIMESTAMP FROM generate_series(1,?) n`, routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&a).Update("http_protocol", true).Error; err != nil {
		t.Fatal(err)
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: queries})
	start := time.Now()
	created, err := CreateRotatingProxy(a.ID, dto.RotatingProxyCreateRequest{Name: "large-pool", Protocol: "http"})
	if err != nil || created.AliveProxyCount != routes {
		t.Fatalf("large-pool creation failed: %+v %v", created, err)
	}
	listed, err := ListRotatingProxies(a.ID)
	if err != nil || len(listed) != 1 || listed[0].AliveProxyCount != routes {
		t.Fatalf("large-pool listing failed: %+v %v", listed, err)
	}
	for _, query := range queries.queries {
		if strings.Contains(query, "username_encrypted") || strings.Contains(query, "password_encrypted") {
			t.Fatalf("rotator count loaded credentials: %s", query)
		}
	}
	t.Logf("creation and listing counted %d candidates without credential loading in %s", routes, time.Since(start))
}

func TestRotatorUptimeScopesHistoryPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, b, p, j := seedCheckerEvidenceTest(t, db)
	sa, err := LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	stat := domain.ProxyStatistic{ProxyID: p.ID, ProtocolID: 1, JudgeID: j.ID, Alive: true, TransportProtocol: "tcp", CheckTimeout: 1000, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: sa.Default.Keys[0], Alive: true}}, WorkspaceIDs: []uint{a.ID}, CreatedAt: time.Now().UTC()}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 1); err != nil {
		t.Fatal(err)
	}
	// Every additional row belongs to a different workspace and route.
	if err := db.Exec(`INSERT INTO proxies (id,host,port,country,estimated_type) VALUES (2,'192.0.2.2',8080,'','')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) VALUES (?,2,'active')`, b.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxy_statistics (alive,attempt,response_time,transport_protocol,check_timeout,check_retries,check_evidence,protocol_id,proxy_id,judge_id,created_at) SELECT TRUE,0,50,'tcp',1000,0,jsonb_build_array(jsonb_build_object('workspace_id',?::bigint,'config_key','unrelated','alive',true)),1,2,?,CURRENT_TIMESTAMP FROM generate_series(1,200000) n`, b.ID, j.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	threshold := float64(80)
	var selected domain.Proxy
	sql := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return buildAliveProxyQuery(tx, a.ID, 1, nil, uptimeFilterMin, &threshold).Order("proxies.id").Limit(1).Find(&selected)
	})
	var encoded string
	if err := db.Raw("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + sql).Row().Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var plan []struct {
		Plan          map[string]any
		ExecutionTime float64 `json:"Execution Time"`
	}
	if err := json.Unmarshal([]byte(encoded), &plan); err != nil {
		t.Fatal(err)
	}
	var scanned float64
	var visit func(map[string]any)
	visit = func(node map[string]any) {
		if node["Relation Name"] == "proxy_statistics" {
			rows, _ := node["Actual Rows"].(float64)
			loops, _ := node["Actual Loops"].(float64)
			scanned += rows * loops
		}
		if children, ok := node["Plans"].([]any); ok {
			for _, child := range children {
				visit(child.(map[string]any))
			}
		}
	}
	visit(plan[0].Plan)
	t.Logf("target_workspace_routes=1 unrelated_history_rows=200000 history_rows_scanned=%.0f execution_ms=%.3f", scanned, plan[0].ExecutionTime)
	if scanned > 1 {
		t.Fatal("uptime read unrelated route history", scanned)
	}
	candidate, err := nextAliveProxyForProtocol(db, a.ID, 1, nil, uptimeFilterMin, &threshold, nil)
	if err != nil || candidate.ID != p.ID {
		t.Fatalf("candidate=%+v error=%v", candidate, err)
	}
}

func TestSourceStatisticsRefreshCoalescesLargePoolPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, p, _ := seedCheckerEvidenceTest(t, db)
	const routes = 10000
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	site := domain.ScrapeSite{URL: "https://example.test/review"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: a.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'192.0.2.1',8080,'','' FROM generate_series(2,?::bigint) n`, routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(2,?::bigint) n`, a.ID, routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxy_scrape_site (proxy_id,scrape_site_id) SELECT n,? FROM generate_series(1,?::bigint) n`, site.ID, routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	sourceStatsDirtyMu.Lock()
	previousDirty := sourceStatsDirty
	sourceStatsDirty = make(map[sourceStatsKey]struct{})
	sourceStatsDirtyMu.Unlock()
	t.Cleanup(func() { sourceStatsDirtyMu.Lock(); sourceStatsDirty = previousDirty; sourceStatsDirtyMu.Unlock() })
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	measured := db.Session(&gorm.Session{Logger: queries})
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := refreshUserScrapeSourceStatsForProxyIDs(measured, []uint64{p.ID}); err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range queries.queries {
		if strings.Contains(query, "INSERT INTO user_scrape_source_stats") {
			t.Fatal("route refresh recounted the source")
		}
	}
	sourceStatsDirtyMu.Lock()
	pending := len(sourceStatsDirty)
	sourceStatsDirtyMu.Unlock()
	if pending != 1 {
		t.Fatalf("10 changes produced %d pending source/workspace pairs, want 1", pending)
	}
	t.Logf("ten one-route source notifications, pool=%d, duration=%s, aggregate scans=0", routes, time.Since(start))
	DB = measured
	flushDirtySourceStats(context.Background())
	var stat domain.WorkspaceScrapeSourceStat
	if err := db.First(&stat, "workspace_id = ? AND scrape_site_id = ?", a.ID, site.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stat.ProxyCount != routes {
		t.Fatalf("coalesced source count=%d, want %d", stat.ProxyCount, routes)
	}
	aggregates := 0
	for _, query := range queries.queries {
		if strings.Contains(query, "INSERT INTO user_scrape_source_stats") {
			aggregates++
		}
	}
	if aggregates != 1 {
		t.Fatalf("coalesced notifications ran %d aggregate refreshes, want 1", aggregates)
	}
}
