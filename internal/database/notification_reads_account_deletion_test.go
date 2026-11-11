package database

import (
	"context"
	"testing"

	"magpie/internal/account"
	"magpie/internal/domain"
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
