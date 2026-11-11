package scrapestore

import (
	"fmt"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	"magpie/internal/platform/schema"
	"magpie/internal/security"
	testdb "magpie/internal/testkit/db"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// setupScrapeStorePostgres opens an isolated, migrated PostgreSQL schema from
// the test kit and makes it the shared pool handle for one test. It skips
// outside integration mode.
func setupScrapeStorePostgres(t testing.TB) *gorm.DB {
	t.Helper()
	db := testdb.OpenPostgres(t)
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	return db
}

// setupProxyTagTestDB is a copy of the core's proxy-tag test database, for the
// source tag tests that moved here with the scraper store.
func setupProxyTagTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-tag-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open tag test database: %v", err)
	}
	if err := schema.ConfigureWorkspaceJoinTables(db); err != nil {
		t.Fatalf("configure workspace join tables: %v", err)
	}
	if err := db.AutoMigrate(&domain.ManagedProxy{}); err != nil {
		t.Fatalf("migrate managed proxy schema: %v", err)
	}
	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Workspace{},
		&domain.WorkspaceMembership{},
		&domain.WorkspaceSubscription{},
		&domain.Proxy{},
		&domain.ProxyTag{},
		&domain.ProxyTagAssignment{},
		&domain.UserProxyFilterIndex{},
	); err != nil {
		t.Fatalf("migrate tag schema: %v", err)
	}

	previousDB := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previousDB })
	return db
}
