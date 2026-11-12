package app

import (
	"context"
	"database/sql"
	"fmt"

	"gorm.io/driver/postgres"

	"magpie/internal/database"
	"magpie/internal/platform/pg"
	"magpie/internal/readmodel"
)

func rebuildReadModels(ctx context.Context) error {
	var configurationError error
	var connection *sql.DB
	_, err := database.SetupDB(func(cfg *database.Config) {
		cfg.AutoMigrate = false
		cfg.SeedDefaults = false
		dialector, ok := cfg.Dialector.(*postgres.Dialector)
		if !ok {
			configurationError = fmt.Errorf("read-model rebuild requires a PostgreSQL dialector")
			cfg.Dialector = nil
			return
		}
		configured, opened, openError := pg.OpenCommandConnection(ctx, dialector.DSN, cfg.Logger)
		connection = opened
		switch {
		case openError != nil && opened == nil:
			configurationError = fmt.Errorf("read-model rebuild database configuration: %w", openError)
		case openError != nil:
			configurationError = openError
		default:
			cfg.ExistingDB = configured
		}
		cfg.Dialector = nil
	})
	if connection != nil {
		defer connection.Close()
	}
	if configurationError != nil {
		return configurationError
	}
	if err != nil {
		return err
	}
	if err := connection.PingContext(ctx); err != nil {
		return fmt.Errorf("read-model rebuild database connection: %w", err)
	}
	if err := readmodel.RefreshReadModelIndexes(ctx); err != nil {
		return err
	}
	return ctx.Err()
}
