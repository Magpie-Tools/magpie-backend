package database

import (
	"testing"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

func createTestWorkspaceForUser(t *testing.T, db *gorm.DB, user domain.User) domain.Workspace {
	t.Helper()
	return testdb.CreateWorkspaceForUser(t, db, user)
}
