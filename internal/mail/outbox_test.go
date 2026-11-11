package mail

import (
	"context"
	"gorm.io/gorm"
	"strings"
	"testing"
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"
)

// setupOutboxPostgresTest opens an isolated, migrated PostgreSQL schema and
// makes it the core handle the outbox runs on.
func setupOutboxPostgresTest(t testing.TB) *gorm.DB {
	t.Helper()
	db := testdb.OpenPostgres(t)
	previous := database.DB
	database.DB = db
	t.Cleanup(func() { database.DB = previous })
	return db
}

func TestBackendLongevityAbandonedEmailRetention(t *testing.T) {
	db := setupOutboxPostgresTest(t)
	old := time.Now().UTC().Add(-180 * 24 * time.Hour)
	rows := []domain.EmailOutbox{{Kind: "password_reset", ToAddress: "fixture@example.invalid", Subject: "fixture", Body: strings.Repeat("x", 8192), Status: domain.EmailOutboxStatusAbandoned, CreatedAt: old, UpdatedAt: old}, {Kind: "password_reset", ToAddress: "fixture@example.invalid", Subject: "fixture", Body: "sent", Status: domain.EmailOutboxStatusSent, SentAt: &old, CreatedAt: old, UpdatedAt: old}}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	deleted, err := DeleteSentEmailOutbox(context.Background(), time.Now().UTC().Add(-24*time.Hour), 500)
	if err != nil {
		t.Fatal(err)
	}
	var abandoned int64
	if err := db.Model(&domain.EmailOutbox{}).Where("status = ?", domain.EmailOutboxStatusAbandoned).Count(&abandoned).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("old_sent_messages_removed=%d old_abandoned_messages_retained=%d", deleted, abandoned)
	if abandoned != 0 {
		t.Fatal("terminal failed mail is retained beyond outbox retention")
	}
}
