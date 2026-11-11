package geolite

import (
	"context"
	"os"
	"testing"
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	testdb "magpie/internal/testkit/db"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// setupGeoLitePostgres opens an isolated, migrated PostgreSQL schema from the
// test kit and makes it the shared pool handle for one test. It skips outside
// integration mode.
func setupGeoLitePostgres(t testing.TB) *gorm.DB {
	t.Helper()
	db := testdb.OpenPostgres(t)
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	return db
}

// The helpers below are copies of the core's and the runtime's test helpers
// of the same names, for the tests that moved here with GeoLite.

func setupIPGeolocationTest(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&domain.IPGeolocationLookup{}, &domain.IPGeolocationState{}); err != nil {
		t.Fatal(err)
	}
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	if _, err := database.GetIPGeolocationState(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

type checkerScaleQueryLogger struct {
	logger.Interface
	queries []string
}

func (l *checkerScaleQueryLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	l.queries = append(l.queries, sql)
	l.Interface.Trace(ctx, begin, fc, err)
}

func withTempRuntimeWorkingDir(t *testing.T) {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chdir(cwd)
	})
}
