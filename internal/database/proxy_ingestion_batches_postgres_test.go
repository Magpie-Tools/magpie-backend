package database

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupProxyIngestionPostgresTest(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("MAGPIE_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN is not set")
	}
	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-ingestion-batch-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, err := admin.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := fmt.Sprintf("proxy_ingestion_batches_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	// Keep pgx's extended protocol enabled so oversized parameter lists fail here.
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := configureWorkspaceJoinTables(db); err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(defaultMigrations()...); err != nil {
		t.Fatal(err)
	}
	previous := DB
	DB = db
	t.Cleanup(func() { DB = previous })
	return db
}

func TestProxyIngestionBatchesPostgres(t *testing.T) {
	// Exercise the old unbatched threshold, both sides of it, and a hash lookup
	// larger than the PostgreSQL extended-protocol parameter limit.
	for _, count := range []int{6000, 8191, 8192, 65536} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db := setupProxyIngestionPostgresTest(t)
			workspace := domain.Workspace{Name: "Batch regression"}
			if err := db.Create(&workspace).Error; err != nil {
				t.Fatal(err)
			}
			subscription := domain.WorkspaceSubscription{WorkspaceID: workspace.ID, OverageMode: domain.WorkspaceOverageUnlimited}
			if err := db.Create(&subscription).Error; err != nil {
				t.Fatal(err)
			}
			site := domain.ScrapeSite{URL: "https://example.test/large-list"}
			if err := db.Create(&site).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
				t.Fatal(err)
			}
			proxies := make([]domain.Proxy, count)
			for i := range proxies {
				proxies[i] = domain.Proxy{IP: fmt.Sprintf("10.%d.%d.%d", i>>16, (i>>8)&255, i&255), Port: 8080}
			}
			proxies[0].Username, proxies[0].Password = "batch-user", "batch-password"
			saved, err := InsertAndGetProxiesWithWorkspace(proxies, workspace.ID)
			if err != nil {
				t.Fatalf("ingest %d proxies: %v", count, err)
			}
			if len(saved) != count {
				t.Fatalf("returned %d proxies, want %d", len(saved), count)
			}
			for _, proxy := range saved {
				if proxy.ID == 0 || len(proxy.Workspaces) != 1 || proxy.Workspaces[0].ID != workspace.ID {
					t.Fatalf("missing ID or active workspace for proxy %d", proxy.ID)
				}
			}
			if err := AssociateProxiesToScrapeSite(site.ID, saved); err != nil {
				t.Fatalf("associate source: %v", err)
			}
			var sourceStat domain.WorkspaceScrapeSourceStat
			if err := db.Where("workspace_id = ? AND scrape_site_id = ?", workspace.ID, site.ID).First(&sourceStat).Error; err != nil {
				t.Fatal(err)
			}
			if int(sourceStat.ProxyCount) != count {
				t.Fatalf("source count = %d, want %d", sourceStat.ProxyCount, count)
			}
			var managed domain.ManagedProxy
			if err := db.Where("workspace_id = ? AND proxy_id = ?", workspace.ID, saved[0].ID).First(&managed).Error; err != nil {
				t.Fatal(err)
			}
			if managed.Username != "batch-user" || managed.Password != "batch-password" {
				t.Fatal("credentials lost during batched import")
			}

			if count == 65536 {
				// Re-scraping must preserve existing workspace lifecycle choices and avoid
				// duplicate routes/associations across lookup and insert batch boundaries.
				if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND proxy_id = ?", workspace.ID, saved[0].ID).
					Updates(map[string]any{"state": domain.ManagedProxyStatePaused, "pause_reason": domain.ManagedProxyPauseReasonManual}).Error; err != nil {
					t.Fatal(err)
				}
				savedAgain, err := InsertAndGetProxiesWithWorkspace(proxies, workspace.ID)
				if err != nil {
					t.Fatalf("reimport: %v", err)
				}
				if len(savedAgain) != count || len(savedAgain[0].Workspaces) != 0 {
					t.Fatal("reimport lost routes or reactivated a paused route")
				}
				if err := AssociateProxiesToScrapeSite(site.ID, savedAgain); err != nil {
					t.Fatal(err)
				}
				for _, model := range []any{&domain.Proxy{}, &domain.ManagedProxy{}, &domain.ProxyScrapeSite{}, &domain.WorkspaceProxyFilterIndex{}} {
					var total int64
					if err := db.Model(model).Count(&total).Error; err != nil {
						t.Fatal(err)
					}
					if total != int64(count) {
						t.Fatalf("%T count = %d, want %d", model, total, count)
					}
				}
				// Cover the fallback ID lookup, even though PostgreSQL normally returns IDs.
				for i := range savedAgain {
					savedAgain[i].ID = 0
				}
				if err := ensureProxyIDs(db, savedAgain); err != nil {
					t.Fatalf("recover IDs: %v", err)
				}
				for _, proxy := range savedAgain {
					if proxy.ID == 0 {
						t.Fatal("missing recovered proxy ID")
					}
				}
			}
		})
	}
}
