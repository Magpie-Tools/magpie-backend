package database

import (
	"context"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"magpie/internal/domain"
	"path/filepath"
	"testing"
	"time"
)

func TestScrapeSourceModeMigrationAndWorkspaceIsolation(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "scraper.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	original := DB
	DB = db
	t.Cleanup(func() { DB = original; sqlDB, _ := db.DB(); sqlDB.Close() })
	// Model the old join table, then run the same model migration used on upgrade.
	if err := db.Exec("CREATE TABLE user_scrape_site (workspace_id integer, scrape_site_id integer, created_at datetime, PRIMARY KEY(workspace_id, scrape_site_id))").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO user_scrape_site(workspace_id,scrape_site_id) VALUES (1,9),(2,9)").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	subscriptions, err := GetScrapeSiteSubscriptions(context.Background(), 9)
	if err != nil || len(subscriptions) != 2 {
		t.Fatalf("subscriptions=%v err=%v", subscriptions, err)
	}
	for _, s := range subscriptions {
		if s.FetchMode != domain.ScrapeFetchBrowser {
			t.Fatal("migration changed an existing source to HTTP")
		}
	}
	updated, err := UpdateScrapeSourceFetchMode(context.Background(), 1, 9, domain.ScrapeFetchHTTP)
	if err != nil || !updated {
		t.Fatalf("update=%v err=%v", updated, err)
	}
	if updated, err := UpdateScrapeSourceFetchMode(context.Background(), 3, 9, domain.ScrapeFetchHTTP); err != nil || updated {
		t.Fatal("unrelated workspace could update a source")
	}
	attempted := time.Now().UTC()
	// An already-running browser scrape may complete after workspace 1 switches
	// to HTTP. It must not overwrite the new mode's status.
	if err := RecordScrapeResult(context.Background(), 9, []uint{1, 2}, domain.ScrapeFetchBrowser, "success", 7, nil, attempted); err != nil {
		t.Fatal(err)
	}
	subscriptions, err = GetScrapeSiteSubscriptions(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range subscriptions {
		if s.WorkspaceID == 1 && (s.FetchMode != domain.ScrapeFetchHTTP || s.LastScrapeStatus != "") {
			t.Fatalf("workspace 1 changed: %+v", s)
		}
		if s.WorkspaceID == 2 && (s.FetchMode != domain.ScrapeFetchBrowser || s.LastScrapeProxyCount != 7) {
			t.Fatalf("workspace 2 changed: %+v", s)
		}
	}
	if err := RecordScrapeResult(context.Background(), 9, []uint{2}, domain.ScrapeFetchBrowser, "empty", 0, nil, attempted.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	subscriptions, _ = GetScrapeSiteSubscriptions(context.Background(), 9)
	for _, s := range subscriptions {
		if s.WorkspaceID == 2 && s.LastScrapeStatus != "success" {
			t.Fatal("older scrape overwrote newer status")
		}
	}
}

func TestNewSourceModesAndStatusAreReturnedPerWorkspace(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "sources.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	original := DB
	DB = db
	t.Cleanup(func() { DB = original; sqlDB, _ := db.DB(); sqlDB.Close() })
	if err := configureWorkspaceJoinTables(db); err != nil {
		t.Fatal(err)
	}
	// The import path only needs these tables. Create the read model afterwards,
	// since its refresh SQL uses PostgreSQL's split_part function.
	for _, sql := range []string{
		"CREATE TABLE workspaces (id integer PRIMARY KEY, name text)",
		"INSERT INTO workspaces(id,name) VALUES (1,'First'),(2,'Second')",
		"CREATE TABLE scrape_sites (id integer PRIMARY KEY AUTOINCREMENT, url text UNIQUE, created_at datetime)",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AutoMigrate(&domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	url := "https://example.com/proxies"
	first, err := SaveScrapingSourcesOfUsers(1, []string{url})
	if err != nil || len(first) != 1 {
		t.Fatalf("HTTP import: %v %v", first, err)
	}
	second, err := SaveScrapingSourcesWithMode(2, []string{url}, domain.ScrapeFetchBrowser)
	if err != nil || len(second) != 1 {
		t.Fatalf("browser import: %v %v", second, err)
	}
	if first[0].ID != second[0].ID {
		t.Fatal("mode duplicated the shared source URL")
	}
	duplicate, err := SaveScrapingSourcesWithMode(1, []string{url}, domain.ScrapeFetchBrowser)
	if err != nil || len(duplicate) != 0 {
		t.Fatal("re-import should preserve existing settings")
	}
	if err := db.AutoMigrate(&domain.WorkspaceScrapeSourceStat{}); err != nil {
		t.Fatal(err)
	}
	for _, workspaceID := range []uint{1, 2} {
		if err := db.Create(&domain.WorkspaceScrapeSourceStat{WorkspaceID: workspaceID, ScrapeSiteID: first[0].ID, URL: url, AddedAt: time.Now()}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		"CREATE TABLE proxy_scrape_site (proxy_id integer, scrape_site_id integer, created_at datetime)",
		"CREATE TABLE user_proxies (proxy_id integer, workspace_id integer)",
		"CREATE TABLE proxy_overall_statuses (proxy_id integer, overall_alive boolean, last_checked_at datetime)",
		"CREATE TABLE proxy_reputations (proxy_id integer, kind text, score real, label text)",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := RecordScrapeResult(context.Background(), first[0].ID, []uint{1}, domain.ScrapeFetchHTTP, "empty", 0, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	for workspaceID, mode := range map[uint]string{1: domain.ScrapeFetchHTTP, 2: domain.ScrapeFetchBrowser} {
		page := GetScrapeSiteInfoPage(workspaceID, 1)
		if len(page) != 1 || page[0].FetchMode != mode {
			t.Fatalf("list for workspace %d: %+v", workspaceID, page)
		}
		detail, err := GetScrapeSiteDetail(workspaceID, first[0].ID)
		if err != nil || detail == nil || detail.FetchMode != mode {
			t.Fatalf("detail for workspace %d: %+v %v", workspaceID, detail, err)
		}
		if workspaceID == 1 && (detail.LastScrapeStatus != "empty" || detail.LastScrapedAt == nil || page[0].LastScrapeStatus != "empty") {
			t.Fatalf("missing attempt status: %+v", detail)
		}
		if workspaceID == 2 && detail.LastScrapeStatus != "" {
			t.Fatal("workspace 1's status leaked to workspace 2")
		}
	}
}
