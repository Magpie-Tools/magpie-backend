package workspace

import (
	"testing"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

// Registration calls CreatePersonalWorkspaceForUser inside its own
// transaction. The test goes through that entry point on SQLite and
// PostgreSQL and checks that the caller's rollback removes the workspace, the
// membership, the preference and the subscription, and that the function left
// the transaction open.
func TestCreatePersonalWorkspaceForUserRollsBackWithItsCaller(t *testing.T) {
	backends := []struct {
		name string
		open func(testing.TB) *gorm.DB
	}{{"sqlite", testdb.OpenSQLite}, {"postgres", testdb.OpenPostgres}}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.open(t)
			user := domain.User{Email: "personal-rollback@example.test", Password: "hash", Role: "user"}
			if err := db.Create(&user).Error; err != nil {
				t.Fatal(err)
			}
			rows := func(handle *gorm.DB) (total int64) {
				t.Helper()
				for _, model := range []any{&domain.Workspace{}, &domain.WorkspaceMembership{}, &domain.WorkspaceMemberPreference{}, &domain.WorkspaceSubscription{}} {
					var count int64
					if err := handle.Model(model).Count(&count).Error; err != nil {
						t.Fatal(err)
					}
					total += count
				}
				return total
			}

			tx := db.Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			if _, err := CreatePersonalWorkspaceForUser(tx, user); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if inside := rows(tx); inside != 4 {
				_ = tx.Rollback()
				t.Fatalf("inside the transaction: %d rows, want 4", inside)
			}
			if err := tx.Rollback().Error; err != nil {
				t.Fatalf("the caller's transaction was already ended: %v", err)
			}
			if after := rows(db); after != 0 {
				t.Fatalf("after the caller's rollback: %d rows, want 0", after)
			}
		})
	}
}
