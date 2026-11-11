package database

import (
	"fmt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"context"
	"testing"

	"magpie/internal/account"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
)

func TestNotificationReadsAreRemovedWithAccountDeletion(t *testing.T) {
	db := setupWorkspaceInvitationTestDB(t)
	if err := db.AutoMigrate(&domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	user := createWorkspaceInvitationUser(t, db, "delete-reader@example.test")
	if err := account.MarkNotificationRead(context.Background(), user.ID, "release-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := DeleteUserAccountWithResult(context.Background(), user.ID); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&domain.NotificationRead{}).Where("user_id = ?", user.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("account deletion retained receipts: %d, %v", count, err)
	}
}

// Copies of the workspace invitation test helpers, for this cross-domain test.

func setupWorkspaceInvitationTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	previous := pg.DB
	t.Cleanup(func() { pg.DB = previous })
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Workspace{},
		&domain.WorkspaceMembership{},
		&domain.WorkspaceMemberPreference{},
		&domain.WorkspaceInvitation{},
	); err != nil {
		t.Fatalf("migrate workspace invitation schema: %v", err)
	}
	pg.DB = db
	return db
}

func createWorkspaceInvitationUser(t *testing.T, db *gorm.DB, email string) domain.User {
	t.Helper()
	user := domain.User{Email: email, Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user %s: %v", email, err)
	}
	return user
}
