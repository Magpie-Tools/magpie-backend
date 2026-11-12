package testdb

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"magpie/internal/platform/schema"
	"magpie/internal/security"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// EnvPostgresDSN names the DSN that turns integration mode on.
const EnvPostgresDSN = "MAGPIE_TEST_POSTGRES_DSN"

const proxyEncryptionKey = "magpie-test-kit-proxy-encryption-key"

func quiet() *gorm.Config {
	return &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
}

// UseProxyEncryptionKey sets a proxy encryption key for the test and resets the
// cipher before and after it, so no test sees another test's key.
func UseProxyEncryptionKey(t testing.TB) {
	t.Helper()
	t.Setenv("PROXY_ENCRYPTION_KEY", proxyEncryptionKey)
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)
}

// OpenSQLite opens an in-memory SQLite database for one test, with every model
// migrated through platform/schema. The database is discarded with the test.
func OpenSQLite(t testing.TB) *gorm.DB {
	t.Helper()
	UseProxyEncryptionKey(t)
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", url.QueryEscape(t.Name()))
	db, err := gorm.Open(sqlite.Open(dsn), quiet())
	if err != nil {
		t.Fatalf("testdb: open sqlite: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("testdb: sqlite handle: %v", err)
	}
	// The shared in-memory database lives until its last connection closes.
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.Exec("PRAGMA busy_timeout = 5000").Error; err != nil {
		t.Fatalf("testdb: set busy timeout: %v", err)
	}
	if err := schema.MigrateModels(db); err != nil {
		t.Fatalf("testdb: migrate sqlite: %v", err)
	}
	return db
}

// PostgresDSN returns the integration DSN. Outside integration mode it skips
// the test.
func PostgresDSN(t testing.TB) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv(EnvPostgresDSN))
	if dsn == "" {
		t.Skip(EnvPostgresDSN + " is not set")
	}
	return dsn
}

// OpenPostgres opens a PostgreSQL schema that belongs to this test, with every
// model migrated through platform/schema. Outside integration mode it skips
// the test. Inside it, any failure to reach or prepare the database fails the
// test; there is no SQLite fallback.
func OpenPostgres(t testing.TB) *gorm.DB {
	t.Helper()
	dsn := PostgresDSN(t)
	UseProxyEncryptionKey(t)
	db := OpenIsolatedSchema(t, dsn, "magpie_test")
	if err := schema.MigrateModels(db); err != nil {
		t.Fatalf("testdb: migrate postgres: %v", err)
	}
	return db
}

// OpenIsolatedSchema creates an empty schema with a unique name and returns a
// handle whose search_path is that schema. The schema is dropped and the
// handle closed when the test ends. The extended query protocol stays enabled,
// so an oversized parameter list fails as it would in production.
func OpenIsolatedSchema(t testing.TB, dsn, prefix string) *gorm.DB {
	t.Helper()
	admin, err := gorm.Open(postgres.Open(dsn), quiet())
	if err != nil {
		t.Fatalf("testdb: open postgres: %v", err)
	}
	adminSQL, err := admin.DB()
	if err != nil {
		t.Fatalf("testdb: postgres handle: %v", err)
	}
	t.Cleanup(func() { _ = adminSQL.Close() })
	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + name).Error; err != nil {
		t.Fatalf("testdb: create schema: %v", err)
	}
	t.Cleanup(func() { _ = admin.Exec("DROP SCHEMA " + name + " CASCADE").Error })
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+name), quiet())
	if err != nil {
		t.Fatalf("testdb: open schema: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("testdb: schema handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}
