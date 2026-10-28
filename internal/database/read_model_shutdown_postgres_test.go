package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func setupSourceStatsShutdownTest(t *testing.T) (*gorm.DB, []sourceStatsKey) {
	t.Helper()
	db := setupProxyIngestionPostgresTest(t)
	t.Setenv("READ_MODEL_REFRESH_INTERVAL_SECONDS", "3600")
	t.Setenv("SCRAPE_SOURCE_STATS_REFRESH_INTERVAL_SECONDS", "3600")
	t.Setenv("SCRAPE_SOURCE_STATS_REFRESH_BATCH_SIZE", "1")
	readModelDirtyMu.Lock()
	previousProxies := readModelDirtyProxyIDs
	previousRound := readModelDirtyRound
	previousBatch := readModelPendingBatch
	readModelPendingBatch = nil
	readModelDirtyProxyIDs = make(map[uint64]struct{})
	readModelDirtyRound = nil
	readModelDirtyMu.Unlock()
	sourceStatsDirtyMu.Lock()
	previousSources := sourceStatsDirty
	sourceStatsDirty = make(map[sourceStatsKey]struct{})
	sourceStatsDirtyMu.Unlock()
	t.Cleanup(func() {
		readModelDirtyMu.Lock()
		readModelDirtyProxyIDs = previousProxies
		readModelDirtyRound = previousRound
		readModelPendingBatch = previousBatch
		readModelDirtyMu.Unlock()
		sourceStatsDirtyMu.Lock()
		sourceStatsDirty = previousSources
		sourceStatsDirtyMu.Unlock()
	})
	workspace, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	var keys []sourceStatsKey
	for i := 0; i < 3; i++ {
		site := domain.ScrapeSite{URL: fmt.Sprintf("http://shutdown.invalid/source/%d", i)}
		if err := db.Create(&site).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ProxyScrapeSite{ProxyID: proxy.ID, ScrapeSiteID: site.ID}).Error; err != nil {
			t.Fatal(err)
		}
		keys = append(keys, sourceStatsKey{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID})
	}
	if err := RefreshReadModelIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertSourceStatsCounts(t, db, keys, 0, 1)
	snapshot, err := LoadCheckerWorkspace(context.Background(), workspace.ID)
	if err != nil {
		t.Fatal(err)
	}
	statistic := domain.ProxyStatistic{
		ProxyID: proxy.ID, JudgeID: judge.ID, ProtocolID: 1, Alive: true,
		CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: workspace.ID, ConfigKey: snapshot.Default.Keys[0], Alive: true}},
		CreatedAt:     time.Now().UTC(),
	}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{statistic}, 1); err != nil {
		t.Fatal(err)
	}
	return db, keys
}

func assertSourceStatsCounts(t *testing.T, db *gorm.DB, keys []sourceStatsKey, alive, unknown uint) {
	t.Helper()
	for _, key := range keys {
		var stat domain.WorkspaceScrapeSourceStat
		if err := db.First(&stat, "workspace_id = ? AND scrape_site_id = ?", key.WorkspaceID, key.ScrapeSiteID).Error; err != nil {
			t.Fatal(err)
		}
		if stat.ProxyCount != 1 || stat.AliveCount != alive || stat.DeadCount != 0 || stat.UnknownCount != unknown {
			t.Fatalf("source %d counters: proxies=%d alive=%d dead=%d unknown=%d, want 1/%d/0/%d", key.ScrapeSiteID, stat.ProxyCount, stat.AliveCount, stat.DeadCount, stat.UnknownCount, alive, unknown)
		}
	}
}

func TestReadModelShutdownDrainsFinalSourceInvalidationsPostgres(t *testing.T) {
	db, keys := setupSourceStatsShutdownTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	StartReadModelRefreshRoutine(ctx)
	assertSourceStatsCounts(t, db, keys, 1, 0)
	sourceStatsDirtyMu.Lock()
	pending := len(sourceStatsDirty)
	sourceStatsDirtyMu.Unlock()
	if pending != 0 {
		t.Fatalf("shutdown left %d source pairs queued", pending)
	}
}

func TestSourceStatsShutdownRetriesFailedBatchPostgres(t *testing.T) {
	db, keys := setupSourceStatsShutdownTest(t)
	var fail atomic.Bool
	fail.Store(true)
	failed := make(chan struct{}, 8)
	if err := db.Callback().Raw().Before("gorm:raw").Register("test:fail_source_stats_refresh", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_scrape_source_stats") {
			if fail.Load() {
				tx.AddError(errors.New("injected source refresh failure"))
				select {
				case failed <- struct{}{}:
				default:
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		StartReadModelRefreshRoutine(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		fail.Store(false)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("source drain did not finish after recovery")
		}
	})
	start := time.Now()
	for i := 0; i < 2; i++ {
		select {
		case <-failed:
		case <-done:
			t.Fatal("shutdown reported success with a failed source batch")
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown did not retry the failed source batch")
		}
	}
	if time.Since(start) < readModelDrainRetryDelay/2 {
		t.Fatal("source drain retried immediately without backoff")
	}
	assertSourceStatsCounts(t, db, keys, 0, 1)
	fail.Store(false)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("source drain did not recover")
	}
	assertSourceStatsCounts(t, db, keys, 1, 0)
	sourceStatsDirtyMu.Lock()
	pending := len(sourceStatsDirty)
	sourceStatsDirtyMu.Unlock()
	if pending != 0 {
		t.Fatalf("successful source drain left %d pairs", pending)
	}
}

func TestCanceledSourceStatsRefreshRetainsBatchForShutdownPostgres(t *testing.T) {
	db, keys := setupSourceStatsShutdownTest(t)
	flushReadModelDirtyProxyIDs(context.Background(), true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if flushDirtySourceStats(ctx) {
		t.Fatal("canceled source refresh reported success")
	}
	sourceStatsDirtyMu.Lock()
	pending := len(sourceStatsDirty)
	sourceStatsDirtyMu.Unlock()
	if pending != len(keys) {
		t.Fatalf("canceled source refresh lost a batch: pending=%d, want %d", pending, len(keys))
	}
	StartReadModelRefreshRoutine(ctx)
	assertSourceStatsCounts(t, db, keys, 1, 0)
}
