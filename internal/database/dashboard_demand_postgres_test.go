package database

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
)

func demandTestState(t *testing.T) *atomic.Int64 {
	t.Helper()
	previous := dashboardCaches
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	dashboardCaches = newDashboardCacheCoordinator(func() time.Time { return time.Unix(0, clock.Load()) }, evictDashboardCacheKey)
	clear := func() {
		dashboardInfoCache.Range(func(key, _ any) bool { dashboardInfoCache.Delete(key); return true })
		dashboardRecentChecksCache.Range(func(key, _ any) bool { dashboardRecentChecksCache.Delete(key); return true })
		dashboardFastestAliveCache.Range(func(key, _ any) bool { dashboardFastestAliveCache.Delete(key); return true })
	}
	clear()
	t.Cleanup(func() { clear(); dashboardCaches = previous })
	return &clock
}

type demandSQLRecorder struct {
	logger.Interface
	mu      sync.Mutex
	queries []string
}

func (r *demandSQLRecorder) Trace(_ context.Context, _ time.Time, sql func() (string, int64), _ error) {
	query, _ := sql()
	r.mu.Lock()
	r.queries = append(r.queries, query)
	r.mu.Unlock()
}
func (r *demandSQLRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	queries := r.queries
	r.queries = nil
	return queries
}

func TestDashboardDemandRequestedComponentsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspaces := seedCurrentLatestReadTest(t, db)
	clock := demandTestState(t)
	recorder := &demandSQLRecorder{Interface: logger.Default.LogMode(logger.Silent)}
	pg.DB = db.Session(&gorm.Session{Logger: recorder})
	if err := RefreshRequestedDashboardCaches(context.Background()); err != nil {
		t.Fatal(err)
	}
	if queries := recorder.take(); len(queries) != 0 {
		t.Fatal("idle refresh queried database", queries)
	}
	if _, err := GetDashboardInfoContext(context.Background(), workspaces[0].ID); err != nil {
		t.Fatal(err)
	}
	recorder.take()
	if err := RefreshRequestedDashboardCaches(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, query := range recorder.take() {
		if strings.Contains(query, "WITH candidates AS MATERIALIZED") || strings.Contains(query, "ufi.workspace_id = 2") || strings.Contains(query, "workspace_id=2") {
			t.Fatal("requested info refreshed an unrequested list or workspace", query)
		}
	}
	key := dashboardCacheKey{workspaceID: workspaces[0].ID, component: dashboardInfoComponent}
	dashboardCaches.mu.Lock()
	touched := dashboardCaches.entries[key].requestedAt
	dashboardCaches.mu.Unlock()
	if !touched.Equal(time.Unix(0, clock.Load())) {
		t.Fatal("background renewed demand")
	}
	clock.Add(int64(dashboardDemandIdle))
	if err := RefreshRequestedDashboardCaches(context.Background()); err != nil {
		t.Fatal(err)
	}
	if queries := recorder.take(); len(queries) != 0 {
		t.Fatal("expired demand queried database", queries)
	}
	if _, ok := dashboardInfoCache.Load(workspaces[0].ID); ok {
		t.Fatal("idle value survived eviction")
	}
}

func TestDashboardDemandStrictAllWorkspacePrewarmPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspaces := seedCurrentLatestReadTest(t, db)
	extra := domain.Workspace{Name: "Fifth strict prewarm workspace"}
	if err := db.Create(&extra).Error; err != nil {
		t.Fatal(err)
	}
	workspaces = append(workspaces, extra)
	demandTestState(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RefreshDashboardCaches(ctx); err != nil {
		t.Fatal("explicit strict prewarm failed", err)
	}
	for _, workspace := range workspaces {
		if _, ok := dashboardInfoCache.Load(workspace.ID); !ok && !workspace.CheckerDirty {
			t.Fatal("explicit prewarm skipped workspace", workspace.ID)
		}
	}
	if len(dashboardCaches.requestedKeys()) != 0 {
		t.Fatal("forced prewarm registered demand")
	}
}

func TestDashboardDemandConcurrentReadersAndTimerShareLoaderPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspaces := seedCurrentLatestReadTest(t, db)
	demandTestState(t)
	recorder := &demandSQLRecorder{Interface: logger.Default.LogMode(logger.Silent)}
	pg.DB = db.Session(&gorm.Session{Logger: recorder})
	start := make(chan struct{})
	results := make(chan error, 17)
	for i := 0; i < 16; i++ {
		go func() {
			<-start
			_, err := GetDashboardInfoContext(context.Background(), workspaces[0].ID)
			results <- err
		}()
	}
	close(start)
	deadline := time.Now().Add(3 * time.Second)
	key := dashboardCacheKey{workspaceID: workspaces[0].ID, component: dashboardInfoComponent}
	for {
		dashboardCaches.mu.Lock()
		entry := dashboardCaches.entries[key]
		active := entry != nil && entry.flight != nil
		dashboardCaches.mu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cold read did not start flight")
		}
		time.Sleep(time.Millisecond)
	}
	go func() { results <- RefreshRequestedDashboardCaches(context.Background()) }()
	for i := 0; i < 17; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	loads := 0
	for _, query := range recorder.take() {
		if strings.Contains(query, "WITH active_inventory AS MATERIALIZED (") {
			loads++
		}
	}
	if loads != 1 {
		t.Fatal("concurrent readers/timer duplicated dashboard aggregate", loads)
	}
}
