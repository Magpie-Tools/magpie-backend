package schema_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/platform/schema"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type schemaDDLLog struct {
	logger.Interface
	mu  sync.Mutex
	ddl []string
}

func (l *schemaDDLLog) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	if strings.HasPrefix(sql, "ALTER") || strings.HasPrefix(sql, "CREATE") || strings.HasPrefix(sql, "DROP") {
		l.mu.Lock()
		l.ddl = append(l.ddl, sql)
		l.mu.Unlock()
	}
}

func TestDashboardSnapshotSchemaPostgres(t *testing.T) {
	db := testdb.OpenPostgres(t)
	if err := schema.EnsureDashboardSnapshotSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := schema.EnsureDashboardSnapshotSchema(db); err != nil {
		t.Fatal("second run", err)
	}
	// Every backend start runs AutoMigrate. It must not rewrite the table:
	// any ALTER takes an exclusive lock while other instances serve reads.
	log := &schemaDDLLog{Interface: logger.Default.LogMode(logger.Silent)}
	if err := db.Session(&gorm.Session{Logger: log}).AutoMigrate(&domain.DashboardSnapshot{}); err != nil {
		t.Fatal(err)
	}
	if len(log.ddl) != 0 {
		t.Fatalf("repeated AutoMigrate issued DDL: %v", log.ddl)
	}
	workspace := domain.Workspace{Name: "Snapshot owner"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	// The read path inserts the bare key; every other column has a default
	// or is nullable.
	if err := db.Exec(`INSERT INTO dashboard_snapshots (workspace_id, payload_version) VALUES (?, 1)`, workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	var row struct {
		FailureCount     int
		Eligible         bool
		CreatedRecently  bool
		LeaseIndexExists bool
	}
	if err := db.Raw(`SELECT failure_count, COALESCE(next_eligible_at <= now(), true) AS eligible,
		created_at > now() - interval '1 minute' AS created_recently,
		EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dashboard_snapshots' AND indexname = 'idx_dashboard_snapshots_lease_until'
			AND indexdef LIKE '%WHERE (lease_until IS NOT NULL)%') AS lease_index_exists
		FROM dashboard_snapshots WHERE workspace_id = ?`, workspace.ID).Scan(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.FailureCount != 0 || !row.Eligible || !row.CreatedRecently || !row.LeaseIndexExists {
		t.Fatalf("snapshot defaults or partial lease index: %+v", row)
	}
	var first, second int64
	if err := db.Raw(`SELECT nextval('dashboard_lease_epoch_seq')`).Scan(&first).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw(`SELECT nextval('dashboard_lease_epoch_seq')`).Scan(&second).Error; err != nil || second <= first {
		t.Fatalf("lease epochs %d then %d: %v", first, second, err)
	}
	// Snapshot rows cascade with their workspace.
	if err := db.Delete(&domain.Workspace{}, workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	var remaining int64
	if err := db.Model(&domain.DashboardSnapshot{}).Count(&remaining).Error; err != nil || remaining != 0 {
		t.Fatalf("snapshot rows after workspace deletion=%d err=%v", remaining, err)
	}
}
