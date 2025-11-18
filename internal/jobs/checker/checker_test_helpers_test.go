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

	if err := db.AutoMigrate(&domain.User{}, &domain.Proxy{}, &domain.UserProxy{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	database.DB = db
	t.Cleanup(func() {
		database.DB = nil
	})

	return db
}
