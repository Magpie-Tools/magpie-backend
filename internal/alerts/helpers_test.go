package alerts

import (
	"testing"

	"magpie/internal/platform/pg"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

// useAlertsDB makes db the shared pool handle the package runs on for one test.
func useAlertsDB(t testing.TB, db *gorm.DB) *gorm.DB {
	t.Helper()
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	return db
}

// setupAlertsSQLite opens a migrated in-memory database from the test kit.
func setupAlertsSQLite(t *testing.T) *gorm.DB {
	t.Helper()
	return useAlertsDB(t, testdb.OpenSQLite(t))
}

// setupAlertsPostgres opens an isolated, migrated PostgreSQL schema from the
// test kit. It skips outside integration mode.
func setupAlertsPostgres(t testing.TB) *gorm.DB {
	t.Helper()
	return useAlertsDB(t, testdb.OpenPostgres(t))
}
