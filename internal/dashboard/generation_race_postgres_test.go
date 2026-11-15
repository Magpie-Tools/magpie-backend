package dashboard

import (
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	if err := db.Create(&domain.AnonymityLevel{ID: 1, Name: "elite"}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err := database.LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	level := 1
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, LevelID: &level, Alive: true, CreatedAt: time.Now().UTC(), WorkspaceIDs: []uint{a.ID}, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: snapshot.Default.Keys[0], Alive: true}}}
	if err := database.InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	originalVersion := database.CurrentCheckerHealthVersion(a.ID)
	raced := false
	hooks := &checkerCacheRaceLogger{Interface: db.Logger, match: "AS elite_proxies", change: func() {
		raced = true
		// Simulate another instance committing while this query is in flight.
		if err := db.Model(&domain.Workspace{}).Where("id = ?", a.ID).UpdateColumns(map[string]any{"checker_generation": snapshot.Generation + 1, "checker_http_key": "changed"}).Error; err != nil {
			t.Error(err)
		}
	}}
	pg.DB = db.Session(&gorm.Session{Logger: hooks})
	info := RefreshDashboardInfoCache(a.ID)
	if !raced || len(info.JudgeValidProxies) != 0 {
		t.Fatal("in-flight health was published", info)
	}
	if _, ok := dashboardInfoCache.Load(a.ID); ok {
		t.Fatal("obsolete dashboard refresh stored")
	}
	pg.DB = db
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
