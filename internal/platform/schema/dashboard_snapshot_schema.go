package schema

import (
	"magpie/internal/platform/pg"

	"gorm.io/gorm"
)

// EnsureDashboardSnapshotSchema creates the sequence that dashboard lease
// epochs come from (ADR 0017). A sequence, unlike a per-row counter, never
// hands out an epoch twice, even after retention deletes and a later read
// recreates a snapshot row. The table, its workspace foreign key and the
// partial lease index come from the domain.DashboardSnapshot model.
func EnsureDashboardSnapshotSchema(db *gorm.DB) error {
	if !pg.IsPostgresDialector(db) {
		return nil
	}
	return db.Exec(`CREATE SEQUENCE IF NOT EXISTS dashboard_lease_epoch_seq`).Error
}
