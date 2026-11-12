package database

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/platform/schema"

	"gorm.io/gorm"
)

// These tests cover what the core hands to the read model: statistics
// ingestion commits latest evidence and queues the ingested routes, and the
// read model projects exactly those routes. They go through
// InsertProxyStatistics and use only the read model's public functions, never
// its internal state. The read model's own tests build the same hand-off from
// plain rows (read_model_fixtures_test.go).

// drainReadModel finishes all pending read-model work through the public
// shutdown path: the refresh routine, given a context that has already ended,
// drains every queued projection and source refresh and returns. Core tests
// call it after creating the read-model schema and before seeding, so work
// queued by earlier tests runs against an empty schema, and after ingesting,
// to wait for the projection.
func drainReadModel(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		StartReadModelRefreshRoutine(ctx)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the read model did not drain")
	}
}

// ingestionReadModelFixture is workspace a of the checker evidence scenario
// with two active routes, one scrape source holding both, and current read
// models in which neither route has evidence yet.
func ingestionReadModelFixture(t *testing.T) (db *gorm.DB, workspace domain.Workspace, judge domain.Judge, checked, unchecked domain.Proxy, site domain.ScrapeSite) {
	t.Helper()
	t.Setenv("READ_MODEL_REFRESH_INTERVAL_SECONDS", "3600")
	t.Setenv("SCRAPE_SOURCE_STATS_REFRESH_INTERVAL_SECONDS", "3600")
	db = setupProxyIngestionPostgresTest(t)
	if err := schema.EnsureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	drainReadModel(t)
	workspace, other, checked, judge := seedCheckerEvidenceTest(t, db)
	if err := db.Where("workspace_id = ?", other.ID).Delete(&domain.ManagedProxy{}).Error; err != nil {
		t.Fatal(err)
	}
	unchecked = domain.Proxy{IP: "192.0.2.2", Port: 8080}
	if err := db.Create(&unchecked).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: unchecked.ID, State: domain.ManagedProxyStateActive}).Error; err != nil {
		t.Fatal(err)
	}
	site = domain.ScrapeSite{URL: "http://ingestion.invalid/source"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
		t.Fatal(err)
	}
	for _, route := range []domain.Proxy{checked, unchecked} {
		if err := db.Create(&domain.ProxyScrapeSite{ProxyID: route.ID, ScrapeSiteID: site.ID}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := RefreshReadModelIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db, workspace, judge, checked, unchecked, site
}

func sourceStat(t *testing.T, db *gorm.DB, workspaceID uint, siteID uint64) domain.WorkspaceScrapeSourceStat {
	t.Helper()
	var stat domain.WorkspaceScrapeSourceStat
	if err := db.First(&stat, "workspace_id = ? AND scrape_site_id = ?", workspaceID, siteID).Error; err != nil {
		t.Fatal(err)
	}
	return stat
}

func filterRow(t *testing.T, db *gorm.DB, workspaceID uint, proxyID uint64) domain.WorkspaceProxyFilterIndex {
	t.Helper()
	var row domain.WorkspaceProxyFilterIndex
	if err := db.First(&row, "workspace_id = ? AND proxy_id = ?", workspaceID, proxyID).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

// An ingested result reaches the proxy filter projection and the scrape-source
// counts through the read model's drain, and ingestion queues only the routes
// it wrote: evidence that arrives without a notification is not projected.
func TestIngestionQueuesExactlyItsRoutesForTheReadModelPostgres(t *testing.T) {
	db, workspace, judge, checked, unchecked, site := ingestionReadModelFixture(t)
	settings, err := LoadCheckerWorkspace(context.Background(), workspace.ID)
	if err != nil {
		t.Fatal(err)
	}
	key := settings.Default.Keys[0]
	if stat := sourceStat(t, db, workspace.ID, site.ID); stat.ProxyCount != 2 || stat.AliveCount != 0 || stat.UnknownCount != 2 {
		t.Fatalf("source counts before ingestion: proxies=%d alive=%d unknown=%d, want 2/0/2", stat.ProxyCount, stat.AliveCount, stat.UnknownCount)
	}
	checkedAt := time.Now().UTC().Truncate(time.Microsecond)
	statistic := domain.ProxyStatistic{
		ProxyID: checked.ID, JudgeID: judge.ID, ProtocolID: 1, Alive: true, CreatedAt: checkedAt,
		CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: workspace.ID, ConfigKey: key, Alive: true}},
	}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{statistic}, 1); err != nil {
		t.Fatal(err)
	}
	// Evidence for the other route that no ingestion announced.
	if err := db.Create(&domain.ProxyLatestStatistic{
		WorkspaceID: workspace.ID, ConfigKey: key, ProxyID: unchecked.ID, ProtocolID: 1,
		Alive: true, StatisticID: 999, JudgeID: judge.ID, CheckedAt: checkedAt,
	}).Error; err != nil {
		t.Fatal(err)
	}
	drainReadModel(t)

	if row := filterRow(t, db, workspace.ID, checked.ID); !row.Alive || !row.AliveHTTP || !row.LatestCheck.Equal(checkedAt) {
		t.Fatalf("the ingested route was not projected: %+v", row)
	}
	if row := filterRow(t, db, workspace.ID, unchecked.ID); row.Alive || !row.LatestCheck.IsZero() {
		t.Fatalf("a route ingestion did not write was projected: %+v", row)
	}
	// The ingested route's invalidation refreshes its source, and a source's
	// counts are recomputed from the current evidence of all its routes, so
	// they include the unannounced row. Before ingestion both were unknown.
	if stat := sourceStat(t, db, workspace.ID, site.ID); stat.ProxyCount != 2 || stat.AliveCount != 2 || stat.UnknownCount != 0 {
		t.Fatalf("source counts after the drain: proxies=%d alive=%d unknown=%d, want 2/2/0", stat.ProxyCount, stat.AliveCount, stat.UnknownCount)
	}

	// The unannounced evidence was projectable: queued, it is projected.
	QueueReadModelRefreshForProxyIDs([]uint64{unchecked.ID})
	drainReadModel(t)
	if row := filterRow(t, db, workspace.ID, unchecked.ID); !row.Alive || !row.LatestCheck.Equal(checkedAt) {
		t.Fatalf("the queued route was not projected: %+v", row)
	}
}

// A result ingested for a route the read model has already projected replaces
// its evidence on the next drain, and a later drain without new ingestion
// leaves the projection as it is.
func TestIngestedResultReplacesProjectedEvidencePostgres(t *testing.T) {
	db, workspace, judge, checked, _, _ := ingestionReadModelFixture(t)
	settings, err := LoadCheckerWorkspace(context.Background(), workspace.ID)
	if err != nil {
		t.Fatal(err)
	}
	key := settings.Default.Keys[0]
	first := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	for _, result := range []struct {
		at    time.Time
		alive bool
	}{{first, true}, {first.Add(30 * time.Second), false}} {
		statistic := domain.ProxyStatistic{
			ProxyID: checked.ID, JudgeID: judge.ID, ProtocolID: 1, Alive: result.alive, CreatedAt: result.at,
			CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: workspace.ID, ConfigKey: key, Alive: result.alive}},
		}
		if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{statistic}, 1); err != nil {
			t.Fatal(err)
		}
		drainReadModel(t)
		if row := filterRow(t, db, workspace.ID, checked.ID); row.Alive != result.alive || !row.LatestCheck.Equal(result.at) {
			t.Fatalf("after ingesting alive=%t at %s the projection is %+v", result.alive, result.at, row)
		}
	}
	before := filterRow(t, db, workspace.ID, checked.ID)
	drainReadModel(t)
	if after := filterRow(t, db, workspace.ID, checked.ID); !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatal("a drain without new ingestion rewrote the projection", before.UpdatedAt, after.UpdatedAt)
	}
}
