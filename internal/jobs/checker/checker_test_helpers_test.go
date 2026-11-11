package checker

import (
	"context"
	"fmt"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"
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

	pg.DB = db
	t.Cleanup(func() {
		pg.DB = nil
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

// startTestRuntime runs a checker runtime's batchers for one test and stops
// them when the test ends. Each test gets its own batchers.
func startTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	r := NewRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.RunBatchers(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return r
}
