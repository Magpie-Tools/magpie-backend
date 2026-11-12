package database

import (
	"testing"

	"magpie/internal/domain"
	"magpie/internal/readmodel"

	"gorm.io/gorm"
)

// The development seed runs one transaction and calls these core functions on
// it. Each test goes through the exported function inside a caller's
// transaction, checks its writes there, rolls back, and checks they are gone
// and the transaction was left open.

func rollbackSeedFixture(t *testing.T, db *gorm.DB) (domain.Workspace, domain.Proxy) {
	t.Helper()
	workspace := domain.Workspace{Name: "Seed rollback"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	proxy := domain.Proxy{IP: "192.0.2.90", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID}).Error; err != nil {
		t.Fatal(err)
	}
	return workspace, proxy
}

func rollBackCaller(t *testing.T, db *gorm.DB, run func(tx *gorm.DB) error, inside, after func(handle *gorm.DB) int64, want int64) {
	t.Helper()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if err := run(tx); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if got := inside(tx); got != want {
		_ = tx.Rollback()
		t.Fatalf("inside the transaction: %d rows, want %d", got, want)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("the caller's transaction was already ended: %v", err)
	}
	if got := after(db); got != 0 {
		t.Fatalf("after the caller's rollback: %d rows, want 0", got)
	}
}

func TestMarkProxyDailyChecksBackfilledRollsBackWithItsCallerPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	_, proxy := rollbackSeedFixture(t, db)
	count := func(handle *gorm.DB) int64 {
		var rows int64
		if err := handle.Model(&domain.ProxyDailyCheckProxyBackfill{}).Where("proxy_id = ?", proxy.ID).Count(&rows).Error; err != nil {
			t.Fatal(err)
		}
		return rows
	}
	rollBackCaller(t, db, func(tx *gorm.DB) error {
		return MarkProxyDailyChecksBackfilled(tx, []uint64{proxy.ID})
	}, count, count, 1)
}

func TestRefreshWorkspaceProxyFilterIndexesRollsBackWithItsCallerPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace, proxy := rollbackSeedFixture(t, db)
	count := func(handle *gorm.DB) int64 {
		var rows int64
		if err := handle.Model(&domain.WorkspaceProxyFilterIndex{}).Where("workspace_id = ? AND proxy_id = ?", workspace.ID, proxy.ID).Count(&rows).Error; err != nil {
			t.Fatal(err)
		}
		return rows
	}
	rollBackCaller(t, db, func(tx *gorm.DB) error {
		return readmodel.RefreshWorkspaceProxyFilterIndexes(tx, "WHERE up.workspace_id = ?", workspace.ID)
	}, count, count, 1)
}

func TestRefreshWorkspaceScrapeSourceStatsRollsBackWithItsCallerPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace, _ := rollbackSeedFixture(t, db)
	site := domain.ScrapeSite{URL: "https://seed.example.test/list"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
		t.Fatal(err)
	}
	count := func(handle *gorm.DB) int64 {
		var rows int64
		if err := handle.Model(&domain.WorkspaceScrapeSourceStat{}).Where("workspace_id = ?", workspace.ID).Count(&rows).Error; err != nil {
			t.Fatal(err)
		}
		return rows
	}
	rollBackCaller(t, db, func(tx *gorm.DB) error {
		return readmodel.RefreshWorkspaceScrapeSourceStats(tx, "WHERE uss.workspace_id = ?", workspace.ID)
	}, count, count, 1)
}
