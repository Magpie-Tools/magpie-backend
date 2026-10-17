package database

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"magpie/internal/domain"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNotificationReadPersistsAcrossConnectionsAndIsAccountScoped(t *testing.T) {
	previous := DB
	t.Cleanup(func() { DB = previous })
	dsn := filepath.Join(t.TempDir(), "notifications.db") + "?_fk=1"
	open := func() *gorm.DB {
		t.Helper()
		db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	DB = open()
	if err := DB.AutoMigrate(&domain.User{}, &domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	user := domain.User{Email: "reader@example.test", Password: "hash"}
	other := domain.User{Email: "other-reader@example.test", Password: "hash"}
	if err := DB.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := MarkNotificationRead(ctx, user.ID, "release-42"); err != nil {
		t.Fatal(err)
	}
	var first domain.NotificationRead
	if err := DB.First(&first).Error; err != nil {
		t.Fatal(err)
	}
	if err := MarkNotificationRead(ctx, user.ID, "release-42"); err != nil {
		t.Fatal(err)
	}
	var rows []domain.NotificationRead
	if err := DB.Find(&rows).Error; err != nil || len(rows) != 1 || !rows[0].ReadAt.Equal(first.ReadAt) {
		t.Fatalf("repeated read changed receipt: %#v, %v", rows, err)
	}
	if err := MarkNotificationRead(ctx, other.ID, "release-7"); err != nil {
		t.Fatal(err)
	}
	connection, err := DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	DB = open()
	// The additive migration can run again without losing the first read time.
	if err := DB.AutoMigrate(&domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[uint][]string{user.ID: {"release-42"}, other.ID: {"release-7"}} {
		got, err := ReadNotificationIDs(ctx, id, []string{"release-42", "release-7", "alert-42"})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("user %d read IDs = %v, %v, want %v", id, got, err, want)
		}
	}
	if err := DB.Delete(&user).Error; err != nil {
		t.Fatal(err)
	}
	got, err := ReadNotificationIDs(ctx, user.ID, []string{"release-42"})
	if err != nil || len(got) != 0 {
		t.Fatalf("deleted account retained read receipts: %v, %v", got, err)
	}
	connection, _ = DB.DB()
	t.Cleanup(func() { _ = connection.Close() })
}

func TestNotificationIDsRequireCanonicalSupportedPositiveIDs(t *testing.T) {
	for _, id := range []string{"invitation-1", "alert-123", "release-9223372036854775807"} {
		if _, _, ok := domain.ParseNotificationID(id); !ok {
			t.Errorf("valid ID rejected: %q", id)
		}
	}
	for _, id := range []string{"", "unknown-1", "alert-0", "alert-01", "release--1", "release-+1", "invitation-1.0", "alert-9223372036854775808", "alert-1-2", "alert- 1"} {
		if _, _, ok := domain.ParseNotificationID(id); ok {
			t.Errorf("invalid ID accepted: %q", id)
		}
	}
}

func TestConcurrentNotificationReadsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	user := domain.User{Email: "concurrent-reader@example.test", Password: "hash"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	const browsers = 16
	errors := make(chan error, browsers)
	var writers sync.WaitGroup
	for range browsers {
		writers.Go(func() {
			errors <- MarkNotificationRead(context.Background(), user.ID, "release-42")
		})
	}
	writers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var rows []domain.NotificationRead
	if err := db.Find(&rows).Error; err != nil || len(rows) != 1 {
		t.Fatalf("concurrent browsers created duplicate receipts: %#v, %v", rows, err)
	}
	if err := db.AutoMigrate(&domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	var saved domain.NotificationRead
	if err := db.First(&saved).Error; err != nil || !saved.ReadAt.Equal(rows[0].ReadAt) {
		t.Fatalf("migration changed the stored read time: %#v, %v", saved, err)
	}
}

func TestNotificationReadsAreRemovedWithAccountDeletion(t *testing.T) {
	db := setupWorkspaceInvitationTestDB(t)
	if err := db.AutoMigrate(&domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	user := createWorkspaceInvitationUser(t, db, "delete-reader@example.test")
	if err := MarkNotificationRead(context.Background(), user.ID, "release-1"); err != nil {
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
