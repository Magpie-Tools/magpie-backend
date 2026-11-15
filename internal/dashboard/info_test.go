package dashboard

import (
	"context"
	"errors"
	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	testdb "magpie/internal/testkit/db"
	"testing"
	"time"
)

func TestGetDashboardInfoServesCachedSnapshotWithoutDatabase(t *testing.T) {
	const userID = uint(991)
	expected := dto.DashboardInfo{
		TotalChecks:      123,
		TotalChecksWeek:  45,
		TotalScraped:     67,
		TotalScrapedWeek: 8,
	}

	dashboardInfoCache.Store(userID, dashboardInfoCacheEntry{info: expected, completedAt: time.Now()})
	t.Cleanup(func() {
		dashboardInfoCache.Delete(userID)
	})

	previousDB := pg.DB
	pg.DB = nil
	t.Cleanup(func() {
		pg.DB = previousDB
	})

	actual := GetDashboardInfo(userID)
	if actual.TotalChecks != expected.TotalChecks ||
		actual.TotalChecksWeek != expected.TotalChecksWeek ||
		actual.TotalScraped != expected.TotalScraped ||
		actual.TotalScrapedWeek != expected.TotalScrapedWeek {
		t.Fatalf("GetDashboardInfo() = %+v, want cached %+v", actual, expected)
	}
}

func TestDashboardProxyListsServeCachedSnapshotsWithoutDatabase(t *testing.T) {
	const userID = uint(992)
	recentKey := dashboardProxyListCacheKey{WorkspaceID: userID, Limit: defaultRecentProxyChecksLimit}
	fastestKey := dashboardProxyListCacheKey{WorkspaceID: userID, Limit: dashboardFastestAliveLimit}
	expectedRecent := []dto.ProxyRecentCheck{{ID: 12, Port: 8080}}
	expectedFastest := []dto.ProxyFastestAlive{{ID: 34, Port: 1080}}

	dashboardRecentChecksCache.Store(recentKey, dashboardHealthCacheEntry[[]dto.ProxyRecentCheck]{value: expectedRecent, completedAt: time.Now()})
	dashboardFastestAliveCache.Store(fastestKey, dashboardHealthCacheEntry[[]dto.ProxyFastestAlive]{value: expectedFastest, completedAt: time.Now()})
	t.Cleanup(func() {
		dashboardRecentChecksCache.Delete(recentKey)
		dashboardFastestAliveCache.Delete(fastestKey)
	})

	previousDB := pg.DB
	pg.DB = nil
	t.Cleanup(func() {
		pg.DB = previousDB
	})

	recent := GetRecentProxyChecks(userID, defaultRecentProxyChecksLimit)
	if len(recent) != 1 || recent[0].ID != expectedRecent[0].ID {
		t.Fatalf("GetRecentProxyChecks() = %+v, want cached %+v", recent, expectedRecent)
	}

	fastest := GetFastestAliveProxies(userID, dashboardFastestAliveLimit)
	if len(fastest) != 1 || fastest[0].ID != expectedFastest[0].ID {
		t.Fatalf("GetFastestAliveProxies() = %+v, want cached %+v", fastest, expectedFastest)
	}
}

func TestNormalizeDashboardCountry(t *testing.T) {
	tests := []struct {
		name    string
		country string
		want    string
	}{
		{name: "empty", country: "", want: "Unknown"},
		{name: "spaces", country: "   ", want: "Unknown"},
		{name: "n/a", country: "N/A", want: "Unknown"},
		{name: "unknown", country: "unknown", want: "Unknown"},
		{name: "unk", country: "UNK", want: "Unknown"},
		{name: "trim known country", country: "  Germany  ", want: "Germany"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeDashboardCountry(tt.country); got != tt.want {
				t.Fatalf("normalizeDashboardCountry(%q) = %q, want %q", tt.country, got, tt.want)
			}
		})
	}
}

// Without invalidation hooks, the committed workspace row is the only guard:
// a deleted workspace is an error, even while fresh values are cached for it.
func TestDashboardReadsOfDeletedWorkspaceFail(t *testing.T) {
	db := testdb.OpenSQLite(t)
	workspace := domain.Workspace{Name: "Deleted dashboard"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	version := database.CheckerHealthVersion{Generation: workspace.CheckerGeneration, Dirty: workspace.CheckerDirty}
	listKey := dashboardProxyListCacheKey{WorkspaceID: workspace.ID, Limit: 1}
	dashboardInfoCache.Store(workspace.ID, dashboardInfoCacheEntry{version: version, info: dto.DashboardInfo{TotalChecks: 5}, completedAt: time.Now()})
	dashboardRecentChecksCache.Store(listKey, dashboardHealthCacheEntry[[]dto.ProxyRecentCheck]{version: version, value: []dto.ProxyRecentCheck{{ID: 1}}, completedAt: time.Now()})
	dashboardFastestAliveCache.Store(listKey, dashboardHealthCacheEntry[[]dto.ProxyFastestAlive]{version: version, value: []dto.ProxyFastestAlive{{ID: 1}}, completedAt: time.Now()})
	t.Cleanup(func() {
		dashboardInfoCache.Delete(workspace.ID)
		dashboardRecentChecksCache.Delete(listKey)
		dashboardFastestAliveCache.Delete(listKey)
	})
	previousDB := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previousDB })
	if info, err := GetDashboardInfoContext(context.Background(), workspace.ID); err != nil || info.TotalChecks != 5 {
		t.Fatalf("live workspace: %+v %v", info, err)
	}
	if err := db.Delete(&domain.Workspace{}, workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := GetDashboardInfoContext(context.Background(), workspace.ID); !errors.Is(err, errDashboardWorkspaceNotFound) {
		t.Fatalf("info of a deleted workspace: %v", err)
	}
	if _, err := GetRecentProxyChecksContext(context.Background(), workspace.ID, 1); !errors.Is(err, errDashboardWorkspaceNotFound) {
		t.Fatalf("recent checks of a deleted workspace: %v", err)
	}
	if _, err := GetFastestAliveProxiesContext(context.Background(), workspace.ID, 1); !errors.Is(err, errDashboardWorkspaceNotFound) {
		t.Fatalf("fastest proxies of a deleted workspace: %v", err)
	}
}
