package database

import (
	"fmt"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	"magpie/internal/platform/schema"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Config struct {
	ExistingDB   *gorm.DB
	Dialector    gorm.Dialector
	Logger       logger.Interface
	AutoMigrate  bool
	Migrations   []any
	SeedDefaults bool
}

type Option func(*Config)

func SetupDB(opts ...Option) (*gorm.DB, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	switch {
	case cfg.ExistingDB != nil:
		pg.DB = cfg.ExistingDB
	case cfg.Dialector != nil:
		if dsn := pg.BuildDSN(); dsn != "" {
			pg.SetDSN(dsn)
		}
		gormCfg := &gorm.Config{}
		if cfg.Logger != nil {
			gormCfg.Logger = cfg.Logger
		}
		db, err := pg.Open(cfg.Dialector, gormCfg)
		if err != nil {
			return nil, fmt.Errorf("database: open connection: %w", err)
		}
		pg.DB = db
	default:
		return nil, fmt.Errorf("database: no dialector or existing connection provided")
	}

	if pg.DB == nil {
		return nil, fmt.Errorf("database: connection was not configured")
	}
	if err := schema.ConfigureWorkspaceJoinTables(pg.DB); err != nil {
		return nil, fmt.Errorf("database: configure workspace join tables: %w", err)
	}

	if cfg.AutoMigrate {
		if err := schema.PrepareCheckerSettingsSchema(pg.DB); err != nil {
			return nil, err
		}
		if err := schema.PrepareWorkspaceOwnershipMigration(pg.DB); err != nil {
			return nil, fmt.Errorf("database: prepare workspace ownership migration: %w", err)
		}
		// Create the complete join-table shape without traversing Proxy and
		// Workspace relationships. GORM can otherwise create user_proxies as a
		// two-column many-to-many table before it sees ManagedProxy, and SQLite
		// may rebuild referenced tables from that incomplete dependency graph.
		if err := pg.DB.AutoMigrate(&schema.ManagedProxyMigrationRow{}); err != nil {
			return nil, fmt.Errorf("database: migrate managed proxy ownership: %w", err)
		}
	}

	migrations := cfg.Migrations
	if cfg.AutoMigrate && schema.ShouldSkipProxyStatisticsAutoMigrate(pg.DB) {
		migrations = schema.RemoveMigrationModel(cfg.Migrations, domain.ProxyStatistic{})
		log.Warn("Database migration: skipping ProxyStatistic auto-migrate because proxy_statistics is partitioned")
	}

	if cfg.AutoMigrate && len(migrations) > 0 {
		if err := pg.DB.AutoMigrate(migrations...); err != nil {
			return nil, fmt.Errorf("database: auto migrate: %w", err)
		}
		log.Info("Database migration completed.")
	}

	if cfg.SeedDefaults {
		if err := schema.SeedDefaults(pg.DB); err != nil {
			return nil, fmt.Errorf("database: seed defaults: %w", err)
		}
	}

	if cfg.AutoMigrate {
		if err := schema.EnsurePostMigrateSchemas(pg.DB); err != nil {
			return nil, err
		}
		if err := schema.EnsureProxyListPerformanceSchema(pg.DB); err != nil {
			return nil, err
		}
		if err := ensureReadModelBackfill(pg.DB); err != nil {
			return nil, err
		}
	}

	return pg.DB, nil
}

func defaultConfig() Config {
	dsn := pg.BuildDSN()

	pg.SetDSN(dsn)

	return Config{
		Dialector:    postgres.Open(dsn),
		Logger:       pg.SilentLogger(),
		AutoMigrate:  support.GetEnvBool("DB_AUTO_MIGRATE", !config.InProductionMode),
		Migrations:   schema.DefaultMigrations(),
		SeedDefaults: true,
	}
}
