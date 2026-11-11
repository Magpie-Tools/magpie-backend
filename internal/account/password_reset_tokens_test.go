package account

import (
	"testing"
	"time"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

// DeletePasswordResetTokensForUser runs inside its caller's transaction: the
// password-reset handler deletes the tokens in the transaction that changes
// the password. A rollback by the caller must keep the tokens, and the
// function must leave the caller's transaction open.
func TestDeletePasswordResetTokensForUserRollsBackWithItsCaller(t *testing.T) {
	backends := []struct {
		name string
		open func(testing.TB) *gorm.DB
	}{{"sqlite", testdb.OpenSQLite}, {"postgres", testdb.OpenPostgres}}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.open(t)
			user := domain.User{Email: "reset-rollback@example.test", Password: "hash"}
			if err := db.Create(&user).Error; err != nil {
				t.Fatal(err)
			}
			token := domain.PasswordResetToken{UserID: user.ID, TokenHash: "token-hash", ExpiresAt: time.Now().UTC().Add(time.Hour)}
			if err := db.Create(&token).Error; err != nil {
				t.Fatal(err)
			}
			count := func(handle *gorm.DB) int64 {
				t.Helper()
				var tokens int64
				if err := handle.Model(&domain.PasswordResetToken{}).Where("user_id = ?", user.ID).Count(&tokens).Error; err != nil {
					t.Fatal(err)
				}
				return tokens
			}

			tx := db.Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			if err := DeletePasswordResetTokensForUser(tx, user.ID); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if tokens := count(tx); tokens != 0 {
				_ = tx.Rollback()
				t.Fatalf("inside the transaction %d tokens remain, want 0", tokens)
			}
			if err := tx.Rollback().Error; err != nil {
				t.Fatalf("the caller's transaction was already ended: %v", err)
			}
			if tokens := count(db); tokens != 1 {
				t.Fatalf("after the caller's rollback %d tokens remain, want 1", tokens)
			}
		})
	}
}
