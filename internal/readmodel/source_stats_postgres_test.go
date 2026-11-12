package readmodel

import (
	"context"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	"magpie/internal/platform/schema"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

func TestSourceStatisticsRefreshCoalescesLargePoolPostgres(t *testing.T) {
	db := setupReadModelPostgresTest(t)
	a, _, p, _ := testdb.SeedCheckerEvidence(t, db)
	const routes = 10000
	if err := schema.EnsureReadModelSchema(db); err != nil {
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
	queries := &testdb.QueryRecorder{Interface: db.Logger}
	measured := db.Session(&gorm.Session{Logger: queries})
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := refreshWorkspaceScrapeSourceStatsForProxyIDs(measured, []uint64{p.ID}); err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range queries.Queries {
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
	pg.DB = measured
	flushDirtySourceStats(context.Background())
	var stat domain.WorkspaceScrapeSourceStat
	if err := db.First(&stat, "workspace_id = ? AND scrape_site_id = ?", a.ID, site.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stat.ProxyCount != routes {
		t.Fatalf("coalesced source count=%d, want %d", stat.ProxyCount, routes)
	}
	aggregates := 0
	for _, query := range queries.Queries {
		if strings.Contains(query, "INSERT INTO user_scrape_source_stats") {
			aggregates++
		}
	}
	if aggregates != 1 {
		t.Fatalf("coalesced notifications ran %d aggregate refreshes, want 1", aggregates)
	}
}
