package database

import (
	"testing"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

// The read model's tests build their fixtures from the test kit and plain rows,
// never from core code, so they test the read model on its own. What the core
// hands to the read model (latest evidence and the invalidation queued after
// statistics ingestion commits) is written here directly. The core's
// integration tests in ingestion_read_model_postgres_test.go go through
// InsertProxyStatistics instead.

// setupReadModelPostgresTest opens an isolated, migrated PostgreSQL schema from
// the test kit and makes it the package handle for the test.
func setupReadModelPostgresTest(t testing.TB) *gorm.DB {
	t.Helper()
	db := testdb.OpenPostgres(t)
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	return db
}

// readModelCheckerKey is the HTTP checker configuration key of the read-model
// fixtures. The projection only compares a workspace's key with the key of its
// evidence rows, so any non-empty value stands for a compiled configuration.
const readModelCheckerKey = "read-model-http"

// useReadModelCheckerKey leaves workspaceID in the state that loading its
// checker settings leaves: generation and revisions 1 and an HTTP
// configuration key, readModelCheckerKey.
func useReadModelCheckerKey(t testing.TB, db *gorm.DB, workspaceID uint) {
	t.Helper()
	if err := db.Model(&domain.Workspace{}).Where("id = ?", workspaceID).Updates(map[string]any{
		"checker_generation": 1, "checker_revision": 1, "checker_full_revision": 1, "checker_projected_revision": 1,
		"checker_http_key": readModelCheckerKey,
	}).Error; err != nil {
		t.Fatal(err)
	}
}

// recordReadModelEvidence stands in for one ingested check result: the latest
// evidence row statistics ingestion writes and the invalidation it queues for
// the route after the transaction commits.
func recordReadModelEvidence(t testing.TB, db *gorm.DB, row domain.ProxyLatestStatistic) {
	t.Helper()
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	QueueReadModelRefreshForProxyIDs([]uint64{row.ProxyID})
}
