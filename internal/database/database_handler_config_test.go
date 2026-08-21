package database

import (
	"os"
	"testing"

	"magpie/internal/config"
	"magpie/internal/domain"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const envDBAutoMigrate = "DB_AUTO_MIGRATE"

func TestDefaultConfig_AutoMigrateDefaultsByMode(t *testing.T) {
	restoreProductionMode := setProductionModeForTest(t, true)
	defer restoreProductionMode()

	unsetEnvForTest(t, envDBAutoMigrate)

	cfg := defaultConfig()
	if cfg.AutoMigrate {
		t.Fatal("expected AutoMigrate=false in production when env is unset")
	}

	config.SetProductionMode(false)
	cfg = defaultConfig()
	if !cfg.AutoMigrate {
		t.Fatal("expected AutoMigrate=true outside production when env is unset")
	}
}

func TestDefaultConfig_AutoMigrateExplicitOverrideWins(t *testing.T) {
	restoreProductionMode := setProductionModeForTest(t, true)
	defer restoreProductionMode()

	t.Setenv(envDBAutoMigrate, "true")
	cfg := defaultConfig()
	if !cfg.AutoMigrate {
		t.Fatal("expected AutoMigrate=true when DB_AUTO_MIGRATE=true")
	}

	config.SetProductionMode(false)
	t.Setenv(envDBAutoMigrate, "false")
	cfg = defaultConfig()
	if cfg.AutoMigrate {
		t.Fatal("expected AutoMigrate=false when DB_AUTO_MIGRATE=false")
	}
}

func TestSetupDBWithAutoMigrateDisabledDoesNotMutateSchema(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:auto-migrate-disabled?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}

	previousDB := DB
	t.Cleanup(func() { DB = previousDB })

	if _, err := SetupDB(func(cfg *Config) {
		cfg.ExistingDB = db
		cfg.Dialector = nil
		cfg.AutoMigrate = false
		cfg.SeedDefaults = false
		cfg.Migrations = []any{domain.User{}}
	}); err != nil {
		t.Fatalf("setup database: %v", err)
	}
	if db.Migrator().HasTable(&domain.User{}) {
		t.Fatal("SetupDB created a table while AutoMigrate was disabled")
	}
}

func setProductionModeForTest(t *testing.T, production bool) func() {
	t.Helper()

	prev := config.InProductionMode
	config.SetProductionMode(production)

	return func() {
		config.SetProductionMode(prev)
	}
}

func unsetEnvForTest(t *testing.T, key string) {
	t.Helper()

	previousValue, hadValue := os.LookupEnv(key)
	if hadValue {
		t.Cleanup(func() {
			_ = os.Setenv(key, previousValue)
		})
	} else {
		t.Cleanup(func() {
			_ = os.Unsetenv(key)
		})
	}

	_ = os.Unsetenv(key)
}
