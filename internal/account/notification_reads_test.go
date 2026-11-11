package account

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNotificationReadPersistsAcrossConnectionsAndIsAccountScoped(t *testing.T) {
	previous := pg.DB
	t.Cleanup(func() { pg.DB = previous })
	dsn := filepath.Join(t.TempDir(), "notifications.db") + "?_fk=1"
	open := func() *gorm.DB {
		t.Helper()
		db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	pg.DB = open()
	if err := pg.DB.AutoMigrate(&domain.User{}, &domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	user := domain.User{Email: "reader@example.test", Password: "hash"}
	other := domain.User{Email: "other-reader@example.test", Password: "hash"}
	if err := pg.DB.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if err := pg.DB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := MarkNotificationRead(ctx, user.ID, "release-42"); err != nil {
		t.Fatal(err)
	}
	var first domain.NotificationRead
	if err := pg.DB.First(&first).Error; err != nil {
		t.Fatal(err)
	}
	if err := MarkNotificationRead(ctx, user.ID, "release-42"); err != nil {
		t.Fatal(err)
	}
	var rows []domain.NotificationRead
	if err := pg.DB.Find(&rows).Error; err != nil || len(rows) != 1 || !rows[0].ReadAt.Equal(first.ReadAt) {
		t.Fatalf("repeated read changed receipt: %#v, %v", rows, err)
	}
	if err := MarkNotificationRead(ctx, other.ID, "release-7"); err != nil {
		t.Fatal(err)
	}
	connection, err := pg.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	pg.DB = open()
	// The additive migration can run again without losing the first read time.
	if err := pg.DB.AutoMigrate(&domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[uint][]string{user.ID: {"release-42"}, other.ID: {"release-7"}} {
		got, err := ReadNotificationIDs(ctx, id, []string{"release-42", "release-7", "alert-42"})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("user %d read IDs = %v, %v, want %v", id, got, err, want)
		}
	}
	if err := pg.DB.Delete(&user).Error; err != nil {
		t.Fatal(err)
	}
	got, err := ReadNotificationIDs(ctx, user.ID, []string{"release-42"})
	if err != nil || len(got) != 0 {
		t.Fatalf("deleted account retained read receipts: %v, %v", got, err)
	}
	connection, _ = pg.DB.DB()
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
	db := setupAccountPostgres(t)
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
