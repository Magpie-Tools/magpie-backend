package database

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/domain"
)

func seedCheckerTag(t *testing.T, db *gorm.DB, workspaceID uint, name string) domain.ProxyTag {
	t.Helper()
	tag := domain.ProxyTag{WorkspaceID: workspaceID, Name: name, Color: "#22C55E"}
	if err := tag.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&tag).Error; err != nil {
		t.Fatal(err)
	}
	return tag
}

func TestCheckerTagProjectionWritesOnlyChangedRoutesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, _ := seedCheckerEvidenceTest(t, db)
	ruleTag := seedCheckerTag(t, db, a.ID, "Checker")
	classTag := seedCheckerTag(t, db, a.ID, "Classification")
	a.CheckerConfig.Rules = []dto.TagCheckerRule{{TagID: ruleTag.ID, Mode: "remove", Protocols: []string{"http"}}}
	if err := db.Model(&a).Select("CheckerConfig").Updates(a).Error; err != nil {
		t.Fatal(err)
	}
	// A large untagged population must not be rewritten by one tag edit.
	if err := db.Exec(`INSERT INTO proxies (id, host, ip_address, port, country, estimated_type) SELECT n, '192.0.2.2', '192.0.2.2', 8080, '', '' FROM generate_series(100,10099) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?, n,'active' FROM generate_series(100,10099) n`, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE TABLE checker_projection_updates (proxy_id bigint)`,
		`CREATE FUNCTION count_checker_projection_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO checker_projection_updates VALUES (NEW.proxy_id); RETURN NEW; END $$`,
		`CREATE TRIGGER count_checker_projection_updates AFTER UPDATE ON user_proxy_filter_indexes FOR EACH ROW EXECUTE FUNCTION count_checker_projection_update()`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	assertUpdates := func(want int64) {
		t.Helper()
		var got int64
		if err := db.Table("checker_projection_updates").Count(&got).Error; err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("filter rows rewritten=%d want=%d", got, want)
		}
		if err := db.Exec("TRUNCATE checker_projection_updates").Error; err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	if err := AddProxyTagsToProxies(a.ID, []uint64{proxy.ID}, []uint64{classTag.ID}); err != nil {
		t.Fatal(err)
	}
	if currentCheckerHealthVersion(a.ID).Dirty {
		t.Fatal("classification tag invalidated checker")
	}
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	assertUpdates(0)
	t.Logf("classification edit plus explicit reconciliation, 10,001 routes: %s; filter writes=0", time.Since(start))
	if err := AddProxyTagsToProxies(a.ID, []uint64{proxy.ID}, []uint64{ruleTag.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	assertUpdates(1)
	// Keep the checker tag while editing only classification membership.
	if _, err := ReplaceProxyTags(a.ID, proxy.ID, []uint64{ruleTag.ID}); err != nil {
		t.Fatal(err)
	}
	if currentCheckerHealthVersion(a.ID).Dirty {
		t.Fatal("unchanged checker membership invalidated")
	}
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	assertUpdates(0)
	if _, err := ReplaceProxyTags(a.ID, proxy.ID, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	assertUpdates(1)
	// A fresh process reading the already-projected generation writes nothing.
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	assertUpdates(0)
}

func TestCheckerSharedDefaultReversionReloadsAllOverridesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, _ := seedCheckerEvidenceTest(t, db)
	tag := seedCheckerTag(t, db, a.ID, "Inherited checks")
	a.CheckerConfig.Rules = []dto.TagCheckerRule{{TagID: tag.ID, Mode: "add", Protocols: []string{"socks5"}}}
	if err := db.Model(&a).Select("CheckerConfig").Updates(a).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ProxyTagAssignment{WorkspaceID: a.ID, ProxyID: proxy.ID, ProxyTagID: tag.ID}).Error; err != nil {
		t.Fatal(err)
	}
	original, err := LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := checkerconfig.Initialize(ctx, nil, func(context.Context, uint) (*checkerconfig.Workspace, error) { return original, nil }, func(context.Context) ([]uint, error) { return []uint{a.ID}, nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		stopped, stop := context.WithCancel(context.Background())
		stop()
		_ = checkerconfig.Initialize(stopped, nil, nil, func(context.Context) ([]uint, error) { return nil, nil })
	})
	for _, timeout := range []uint16{2000, 1000} {
		settings := *a.CheckerConfig
		settings.Defaults.Timeout = timeout
		if err := UpdateWorkspaceSettings(a.ID, 1, dto.UserSettings{ProvidedFields: map[string]bool{"checker_settings": true}, CheckerSettings: &settings}); err != nil {
			t.Fatal(err)
		}
		current, err := LoadCheckerWorkspace(context.Background(), a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Value.CheckerFullRevision <= original.Value.CheckerRevision || current.Plan(proxy.ID).Settings[3].Timeout != timeout {
			t.Fatal("shared change did not advance the full-refresh watermark or reload inherited settings")
		}
		var persisted domain.ProxyCheckerPlan
		if err := db.Where("workspace_id=? AND proxy_id=? AND protocol_id=4", a.ID, proxy.ID).First(&persisted).Error; err != nil {
			t.Fatal(err)
		}
		if persisted.ConfigKey != current.Plan(proxy.ID).Keys[3] {
			t.Fatal("a missed shared change left an obsolete override")
		}
	}
}

func TestCheckerRemovedMembershipDoesNotSurviveReimportPostgres(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		for _, prune := range []bool{false, true} {
			t.Run(fmt.Sprintf("automatic=%t/prune=%t", automatic, prune), func(t *testing.T) {
				db := setupProxyIngestionPostgresTest(t)
				a, _, proxy, _ := seedCheckerEvidenceTest(t, db)
				if err := ensureCheckerSettingsSchema(db); err != nil {
					t.Fatal(err)
				}
				tag := seedCheckerTag(t, db, a.ID, "Former membership")
				timeout := uint16(2000)
				a.CheckerConfig.Rules = []dto.TagCheckerRule{{TagID: tag.ID, Mode: "add", Protocols: []string{"http"}, Timeout: &timeout}}
				if err := db.Model(&a).Select("CheckerConfig").Updates(a).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.Create(&domain.ProxyTagAssignment{WorkspaceID: a.ID, ProxyID: proxy.ID, ProxyTagID: tag.ID}).Error; err != nil {
					t.Fatal(err)
				}
				original, err := LoadCheckerWorkspace(context.Background(), a.ID)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				// Keep a process at the preceding revision, missing deletion messages.
				if err := checkerconfig.Initialize(ctx, nil, func(context.Context, uint) (*checkerconfig.Workspace, error) { return original, nil }, func(context.Context) ([]uint, error) { return []uint{a.ID}, nil }); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					cancel()
					stopped, stop := context.WithCancel(context.Background())
					stop()
					_ = checkerconfig.Initialize(stopped, nil, nil, func(context.Context) ([]uint, error) { return nil, nil })
				})
				if automatic {
					deleted, _, err := DeleteActiveManagedProxy(a.ID, proxy.ID)
					if err != nil || !deleted {
						t.Fatal("failure deletion failed", deleted, err)
					}
				} else if count, _, err := DeleteProxyRelation(a.ID, []int{int(proxy.ID)}); err != nil || count != 1 {
					t.Fatal("manual deletion failed", count, err)
				}
				if err := db.Model(&domain.CheckerProxyChange{}).Where("workspace_id=?", a.ID).Update("updated_at", time.Now().Add(-48*time.Hour)).Error; err != nil {
					t.Fatal(err)
				}
				if count, err := PruneDeletedCheckerRoutes(context.Background(), time.Now().Add(-24*time.Hour), 10); err != nil || count != 0 {
					t.Fatal("pending removal tombstone was pruned", count, err)
				}
				removed, err := LoadCheckerWorkspace(context.Background(), a.ID)
				if err != nil || removed.Proxies.Len() != 0 || original.Plan(proxy.ID).Settings[0].Timeout != timeout {
					t.Fatal("removal failed or mutated a published plan", err)
				}
				if prune {
					if count, err := PruneDeletedCheckerRoutes(context.Background(), time.Now().Add(-24*time.Hour), 10); err != nil || count != 1 {
						t.Fatal("committed removal did not expire", count, err)
					}
				}
				if err := db.Create(&domain.ManagedProxy{WorkspaceID: a.ID, ProxyID: proxy.ID}).Error; err != nil {
					t.Fatal(err)
				}
				reimported, err := LoadCheckerWorkspace(context.Background(), a.ID)
				if err != nil || reimported.Plan(proxy.ID) != reimported.Default || reimported.Plan(proxy.ID).Settings[0].Timeout != 1000 {
					t.Fatal("reimport reused old checker tags", err)
				}
				if !prune {
					if count, err := PruneDeletedCheckerRoutes(context.Background(), time.Now().Add(-24*time.Hour), 10); err != nil || count != 0 {
						t.Fatal("reimported membership journal was pruned", count, err)
					}
					var change domain.CheckerProxyChange
					if err := db.Where("workspace_id=? AND proxy_id=?", a.ID, proxy.ID).First(&change).Error; err != nil || change.Deleted {
						t.Fatal("reimported membership still marked removed", change, err)
					}
				}
			})
		}
	}
}

func TestCheckerMissedChangeAndReversionReconcilesPersistedProjectionPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	tag := seedCheckerTag(t, db, a.ID, "Timeout")
	timeout := uint16(2000)
	a.CheckerConfig.Rules = []dto.TagCheckerRule{{TagID: tag.ID, Mode: "add", Protocols: []string{"http"}, Timeout: &timeout}}
	if err := db.Model(&a).Select("CheckerConfig").Updates(a).Error; err != nil {
		t.Fatal(err)
	}
	original, err := LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Install the original snapshot as a process that misses both notifications.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := checkerconfig.Initialize(ctx, nil, func(context.Context, uint) (*checkerconfig.Workspace, error) { return original, nil }, func(context.Context) ([]uint, error) { return []uint{a.ID}, nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		stopped, c := context.WithCancel(context.Background())
		c()
		_ = checkerconfig.Initialize(stopped, nil, nil, func(context.Context) ([]uint, error) { return nil, nil })
	})
	if err := AddProxyTagsToProxies(a.ID, []uint64{proxy.ID}, []uint64{tag.ID}); err != nil {
		t.Fatal(err)
	}
	changed, err := LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil || changed.Plan(proxy.ID).Settings[0].Timeout != timeout {
		t.Fatal("missed addition was not projected", changed, err)
	}
	if _, err := ReplaceProxyTags(a.ID, proxy.ID, nil); err != nil {
		t.Fatal(err)
	}
	// Its local snapshot already matches the reverted configuration, but SQL does not.
	reverted, err := LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	var overrides int64
	if err := db.Model(&domain.ProxyCheckerPlan{}).Where("workspace_id = ?", a.ID).Count(&overrides).Error; err != nil || overrides != 0 {
		t.Fatal("stale overrides", overrides, err)
	}
	if reverted.Generation <= original.Generation {
		t.Fatal("persisted generation did not advance")
	}
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, CreatedAt: time.Now().UTC(), WorkspaceIDs: []uint{a.ID}, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: reverted.Default.Keys[0], Alive: true}}}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	detail, err := GetProxyDetail(a.ID, proxy.ID)
	if err != nil || detail.Alive == nil || !*detail.Alive {
		t.Fatal("current success rejected after reversion", detail, err)
	}
}

type checkerCacheRaceLogger struct {
	logger.Interface
	once   sync.Once
	change func()
	match  string
}

func (l *checkerCacheRaceLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	if strings.Contains(sql, l.match) {
		l.once.Do(l.change)
	}
	l.Interface.Trace(ctx, begin, fc, err)
}

func TestDashboardRefreshRejectsInFlightOldGenerationPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	// Keep the unrelated deferred daily backfill out of this health-cache race
	// fixture by marking it already in flight for the fixture's workspace.
	proxyDailyBackfillInFlight.Store(a.ID, struct{}{})
	t.Cleanup(func() { proxyDailyBackfillInFlight.Delete(a.ID) })
	if err := db.Create(&domain.AnonymityLevel{ID: 1, Name: "elite"}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err := LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	level := 1
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, LevelID: &level, Alive: true, CreatedAt: time.Now().UTC(), WorkspaceIDs: []uint{a.ID}, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: snapshot.Default.Keys[0], Alive: true}}}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	originalVersion := currentCheckerHealthVersion(a.ID)
	raced := false
	hooks := &checkerCacheRaceLogger{Interface: db.Logger, match: "AS elite_proxies", change: func() {
		raced = true
		// Simulate another instance committing while this query is in flight.
		if err := db.Model(&domain.Workspace{}).Where("id = ?", a.ID).UpdateColumns(map[string]any{"checker_generation": snapshot.Generation + 1, "checker_http_key": "changed"}).Error; err != nil {
			t.Error(err)
		}
	}}
	DB = db.Session(&gorm.Session{Logger: hooks})
	info := RefreshDashboardInfoCache(a.ID)
	if !raced || len(info.JudgeValidProxies) != 0 {
		t.Fatal("in-flight health was published", info)
	}
	if _, ok := dashboardInfoCache.Load(a.ID); ok {
		t.Fatal("obsolete dashboard refresh stored")
	}
	DB = db
	// A delayed store after invalidation must also be rejected on another instance's read.
	dashboardInfoCache.Store(a.ID, dashboardInfoCacheEntry{version: originalVersion, info: dto.DashboardInfo{TotalChecks: 987654}})
	recentKey := dashboardProxyListCacheKey{WorkspaceID: a.ID, Limit: 1}
	fastestKey := dashboardProxyListCacheKey{WorkspaceID: a.ID, Limit: 1}
	dashboardRecentChecksCache.Store(recentKey, dashboardHealthCacheEntry[[]dto.ProxyRecentCheck]{version: originalVersion, value: []dto.ProxyRecentCheck{{ID: proxy.ID, Alive: true}}})
	dashboardFastestAliveCache.Store(fastestKey, dashboardHealthCacheEntry[[]dto.ProxyFastestAlive]{version: originalVersion, value: []dto.ProxyFastestAlive{{ID: proxy.ID}}})
	t.Cleanup(func() {
		dashboardInfoCache.Delete(a.ID)
		dashboardRecentChecksCache.Delete(recentKey)
		dashboardFastestAliveCache.Delete(fastestKey)
	})
	if got := GetDashboardInfo(a.ID); got.TotalChecks == 987654 || len(got.JudgeValidProxies) > 0 {
		t.Fatal("obsolete dashboard served", got)
	}
	if got := GetRecentProxyChecks(a.ID, 1); len(got) > 0 && got[0].Alive {
		t.Fatal("obsolete recent checks served", got)
	}
	if got := GetFastestAliveProxies(a.ID, 1); len(got) > 0 {
		t.Fatal("obsolete fastest proxies served", got)
	}
}

func TestSmallSourceHealthAggregatesCandidatesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, _ := seedCheckerEvidenceTest(t, db)
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	// Increase this to one million for the review's synthetic workload.
	routes := 10000
	if value := os.Getenv("MAGPIE_TEST_CHECKER_SOURCE_ROUTES"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			t.Fatal("invalid source benchmark size", value)
		}
		routes = parsed
	}
	// Populate enough unrelated evidence to expose an unbounded aggregate plan.
	if err := db.Exec(`INSERT INTO proxies (id, host, ip_address, port, country, estimated_type) SELECT n, '192.0.2.2', '192.0.2.2', 8080, '', '' FROM generate_series(100,?) n`, 99+routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?, n,'active' FROM generate_series(100,?) n`, a.ID, 99+routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,checked_at) SELECT ?, w.checker_http_key,n,1,true,n,CURRENT_TIMESTAMP FROM generate_series(100,?) n CROSS JOIN workspaces w WHERE w.id = ?`, a.ID, 99+routes, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,checked_at) SELECT id,checker_http_key,?,1,true,?,CURRENT_TIMESTAMP FROM workspaces WHERE id=?`, proxy.ID, proxy.ID, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	site := domain.ScrapeSite{URL: "https://example.test/small"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: a.ID, ScrapeSiteID: site.ID, FetchMode: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxy_scrape_site (proxy_id,scrape_site_id,created_at) VALUES (?,?,CURRENT_TIMESTAMP)`, proxy.ID, site.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	capture := &checkerSourceQueryLogger{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: capture})
	detail, err := GetScrapeSiteDetail(a.ID, site.ID)
	DB = db
	if err != nil || detail.ProxyCount != 1 || detail.AliveCount != 1 {
		t.Fatal(fmt.Sprint(detail), err)
	}
	if capture.query == "" {
		t.Fatal("source detail did not query health")
	}
	var lines []string
	if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS) " + capture.query).Scan(&lines).Error; err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	if strings.Contains(plan, "Seq Scan on proxy_latest_statistics") {
		t.Fatal("small source scanned workspace evidence", plan)
	}
	t.Log(plan)

}

type checkerSourceQueryLogger struct {
	logger.Interface
	query string
}

func (l *checkerSourceQueryLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	if strings.Contains(sql, "COUNT(DISTINCT pss.proxy_id) AS proxy_count") {
		l.query = sql
	}
	l.Interface.Trace(ctx, begin, fc, err)
}
