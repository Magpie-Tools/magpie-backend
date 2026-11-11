package pg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var currentDSN atomic.Value

func SetDSN(dsn string) {
	if dsn == "" {
		return
	}
	currentDSN.Store(dsn)
}

func GetDSN() string {
	if raw := currentDSN.Load(); raw != nil {
		if dsn, ok := raw.(string); ok {
			return dsn
		}
	}
	return ""
}

func BuildDSN() string {
	dbHost := support.GetEnv("DB_HOST", "localhost")
	dbPort := support.GetEnv("DB_PORT", "5434")
	dbName := support.GetEnv("DB_NAME", "magpie")
	dbUser := support.GetEnv("DB_USERNAME", "magpie_user")
	// Default to empty instead of a shared hardcoded secret.
	dbPassword := support.GetEnv("DB_PASSWORD", "")
	dbSSLMode := resolveDBSSLMode()

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		dbHost,
		dbPort,
		dbUser,
		dbPassword,
		dbName,
		dbSSLMode,
	)

	return dsn
}

func resolveDBSSLMode() string {
	raw := strings.ToLower(strings.TrimSpace(support.GetEnv("DB_SSLMODE", "require")))

	switch raw {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return raw
	default:
		log.Warn("invalid DB_SSLMODE value, defaulting to secure mode", "value", raw, "default", "require")
		return "require"
	}
}

func SilentLogger() logger.Interface {
	return logger.New(
		log.Default(),
		logger.Config{LogLevel: logger.Silent},
	)
}

// Open creates the GORM pool and applies the pool configuration. It is the
// only place the runtime opens its database connection pool.
func Open(dialector gorm.Dialector, config *gorm.Config) (*gorm.DB, error) {
	db, err := gorm.Open(dialector, config)
	if err != nil {
		return nil, err
	}
	configureConnectionPool(db)
	return db, nil
}

func configureConnectionPool(db *gorm.DB) {
	if db == nil {
		return
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Error("database: get sql.DB", "error", err)
		return
	}

	maxOpen := support.GetEnvInt("DB_MAX_OPEN_CONNS", 32)
	maxIdle := support.GetEnvInt("DB_MAX_IDLE_CONNS", maxOpen)
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}

	connLifetimeSeconds := support.GetEnvInt("DB_CONN_MAX_LIFETIME", 300)
	connIdleSeconds := support.GetEnvInt("DB_CONN_MAX_IDLE_TIME", 60)

	if maxOpen > 0 {
		sqlDB.SetMaxOpenConns(maxOpen)
	}
	if maxIdle >= 0 {
		sqlDB.SetMaxIdleConns(maxIdle)
	}
	if connLifetimeSeconds > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(connLifetimeSeconds) * time.Second)
	}
	if connIdleSeconds > 0 {
		sqlDB.SetConnMaxIdleTime(time.Duration(connIdleSeconds) * time.Second)
	}
}

// OpenCommandConnection opens the connection of a one-shot command that exits
// right after cancellation, bound to the command's signal context. It returns
// the GORM handle and the underlying pool, which the caller closes. A parse
// failure is reported as a configuration error with a nil pool.
func OpenCommandConnection(ctx context.Context, dsn string, gormLogger logger.Interface) (*gorm.DB, *sql.DB, error) {
	pgConfig, parseError := pgx.ParseConfig(dsn)
	if parseError != nil {
		return nil, nil, parseError
	}
	// A command exits immediately after cancellation. pgx's default deadline
	// handler can leave its asynchronous CancelRequest unfinished at exit.
	// Wait for server cancellation here; ordinary runtime connections keep
	// their existing handler and incur no additional work.
	pgConfig.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, DeadlineDelay: 5 * time.Second}
	}
	connection := stdlib.OpenDB(*pgConfig)
	// GORM normally pings with a background context during Open. Keep
	// connection establishment under the command's signal context too.
	configured, openError := gorm.Open(postgres.New(postgres.Config{Conn: connection}), &gorm.Config{
		DisableAutomaticPing: true, Logger: gormLogger,
	})
	if openError != nil {
		return nil, connection, openError
	}
	return configured.WithContext(ctx), connection, nil
}

// ConnectNative opens one native pgx connection outside the pool, for COPY.
// The caller closes it.
func ConnectNative(ctx context.Context, dsn string) (*pgx.Conn, error) {
	return pgx.Connect(ctx, dsn)
}
