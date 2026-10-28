package app

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"magpie/internal/database"
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
		pgConfig, parseError := pgx.ParseConfig(dialector.DSN)
		if parseError != nil {
			configurationError = fmt.Errorf("read-model rebuild database configuration: %w", parseError)
			cfg.Dialector = nil
			return
		}
		// A command exits immediately after cancellation. pgx's default deadline
		// handler can leave its asynchronous CancelRequest unfinished at exit.
		// Wait for server cancellation here; ordinary runtime connections keep
		// their existing handler and incur no additional work.
		pgConfig.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
			return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, DeadlineDelay: 5 * time.Second}
		}
		connection = stdlib.OpenDB(*pgConfig)
		// GORM normally pings with a background context during Open. Keep
		// connection establishment under the command's signal context too.
		configured, openError := gorm.Open(postgres.New(postgres.Config{Conn: connection}), &gorm.Config{
			DisableAutomaticPing: true, Logger: cfg.Logger,
		})
		if openError != nil {
			configurationError = openError
		} else {
			cfg.ExistingDB = configured.WithContext(ctx)
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
	if err := database.RefreshReadModelIndexes(ctx); err != nil {
		return err
	}
	return ctx.Err()
}
