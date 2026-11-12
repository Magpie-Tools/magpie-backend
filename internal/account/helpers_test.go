package account

import (
	"testing"

	"magpie/internal/platform/pg"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

// setupAccountPostgres opens an isolated, migrated PostgreSQL schema from the
// test kit and makes it the shared pool handle for one test. It skips outside
// integration mode.
func setupAccountPostgres(t testing.TB) *gorm.DB {
	t.Helper()
	db := testdb.OpenPostgres(t)
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	return db
}
