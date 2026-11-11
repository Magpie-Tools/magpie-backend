package database

import (
	"testing"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

// The scraper store opens its own transactions and calls these core functions
// on them. Each test goes through the exported entry point and its helpers
// inside a caller's transaction, checks the writes are visible there, rolls
// back, and checks they are gone and the transaction was left open.

func TestMarkCheckerAssignmentsDirtyRollsBackWithItsCallerPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	tag := domain.ProxyTag{Name: "Rollback"}
	workspace := domain.Workspace{Name: "Rollback", CheckerConfig: testCheckerProfiles()}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	tag.WorkspaceID = workspace.ID
	if err := db.Create(&tag).Error; err != nil {
		t.Fatal(err)
	}
	workspace.CheckerConfig.Rules = []dto.TagCheckerRule{{TagID: tag.ID, Mode: "add", Protocols: []string{"http"}}}
	if err := db.Model(&workspace).Update("checker_config", workspace.CheckerConfig).Error; err != nil {
		t.Fatal(err)
	}
	proxy := domain.Proxy{IP: "192.0.2.80", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID}).Error; err != nil {
		t.Fatal(err)
	}
	state := func(handle *gorm.DB) (changes int64, dirty bool, revision uint64) {
		t.Helper()
		if err := handle.Model(&domain.CheckerProxyChange{}).Where("workspace_id = ?", workspace.ID).Count(&changes).Error; err != nil {
			t.Fatal(err)
		}
		var stored domain.Workspace
		if err := handle.First(&stored, workspace.ID).Error; err != nil {
			t.Fatal(err)
		}
		return changes, stored.CheckerDirty, stored.CheckerRevision
	}
	_, _, revision := state(db)

	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	changed, err := MarkCheckerAssignmentsDirty(tx, workspace.ID, []uint64{tag.ID}, []uint64{proxy.ID})
	if err != nil || !changed {
		_ = tx.Rollback()
		t.Fatalf("mark: changed=%v err=%v", changed, err)
	}
	if changes, dirty, inside := state(tx); changes != 1 || !dirty || inside != revision+1 {
		_ = tx.Rollback()
		t.Fatalf("inside the transaction: changes=%d dirty=%v revision=%d", changes, dirty, inside)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("the caller's transaction was already ended: %v", err)
	}
	if changes, dirty, after := state(db); changes != 0 || dirty || after != revision {
		t.Fatalf("after the caller's rollback: changes=%d dirty=%v revision=%d", changes, dirty, after)
	}
}

func TestRefreshWorkspaceScrapeSourceStatsForWorkspaceSitesRollsBackWithItsCallerPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace := domain.Workspace{Name: "Source stats rollback"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	site := domain.ScrapeSite{URL: "https://source.example.test/list"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
		t.Fatal(err)
	}
	proxy := domain.Proxy{IP: "192.0.2.81", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO proxy_scrape_site (proxy_id, scrape_site_id) VALUES (?, ?)", proxy.ID, site.ID).Error; err != nil {
		t.Fatal(err)
	}
	stats := func(handle *gorm.DB) []domain.WorkspaceScrapeSourceStat {
		t.Helper()
		var rows []domain.WorkspaceScrapeSourceStat
		if err := handle.Where("workspace_id = ?", workspace.ID).Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		return rows
	}

	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if err := RefreshWorkspaceScrapeSourceStatsForWorkspaceSites(tx, workspace.ID, []uint64{site.ID}); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if rows := stats(tx); len(rows) != 1 || rows[0].ProxyCount != 1 {
		_ = tx.Rollback()
		t.Fatalf("inside the transaction: %+v", rows)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("the caller's transaction was already ended: %v", err)
	}
	if rows := stats(db); len(rows) != 0 {
		t.Fatalf("after the caller's rollback: %+v", rows)
	}
}
