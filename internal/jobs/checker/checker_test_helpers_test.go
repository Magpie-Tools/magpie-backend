package checker

import (
	"fmt"
	"testing"

	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupCheckerTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	t.Setenv("PROXY_ENCRYPTION_KEY", "checker-test-key")
	security.ResetProxyCipherForTests()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite database: %v", err)
	}

	if err := db.Exec("PRAGMA busy_timeout = 5000").Error; err != nil {
		t.Fatalf("set busy timeout: %v", err)
	}

	if err := db.AutoMigrate(&domain.ManagedProxy{}); err != nil {
		t.Fatalf("auto migrate managed proxy: %v", err)
	}
	if err := db.AutoMigrate(&domain.User{}, &domain.Workspace{}, &domain.Proxy{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	database.DB = db
	t.Cleanup(func() {
		database.DB = nil
	})

	return db
}

func createCheckerWorkspace(t *testing.T, db *gorm.DB, user domain.User) {
	t.Helper()
	workspace := domain.Workspace{
		ID:                         user.ID,
		Name:                       user.Email + " workspace",
		HTTPProtocol:               user.HTTPProtocol,
		HTTPSProtocol:              user.HTTPSProtocol,
		SOCKS4Protocol:             user.SOCKS4Protocol,
		SOCKS5Protocol:             user.SOCKS5Protocol,
		Timeout:                    user.Timeout,
		Retries:                    user.Retries,
		UseHttpsForSocks:           user.UseHttpsForSocks,
		TransportProtocol:          user.TransportProtocol,
		AutoRemoveFailingProxies:   user.AutoRemoveFailingProxies,
		AutoRemoveFailureThreshold: user.AutoRemoveFailureThreshold,
	}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatalf("create checker workspace: %v", err)
	}
}
