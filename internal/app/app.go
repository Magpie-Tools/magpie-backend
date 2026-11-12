package app

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/log"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"magpie/internal/app/bootstrap"
	"magpie/internal/app/server"
	"magpie/internal/auth"
	"magpie/internal/checkpace"
	"magpie/internal/config"
	"magpie/internal/database"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
	"magpie/internal/jobs/runtime"
	"magpie/internal/security"
	"magpie/internal/support"
)

const (
	defaultBackendPort = 5656
)

func Run() error {
	if err := godotenv.Load(); err != nil {
		log.Warn("No .env file found. Falling back to system environment variables.")
	}

	configureLogLevel()

	backendPortFlag := flag.Int("backend-port", defaultBackendPort, "Port for API server")
	productionFlag := flag.Bool("production", false, "Run in production mode")
	migrateOnlyFlag := flag.Bool("migrate-only", false, "Run database migrations and exit")
	rebuildReadModelsFlag := flag.Bool("rebuild-read-models", false, "Rebuild derived proxy and scrape-source indexes and exit; stop writers first")
	flag.Parse()

	if *migrateOnlyFlag && *rebuildReadModelsFlag {
		return errors.New("--migrate-only and --rebuild-read-models cannot be combined")
	}
	config.SetProductionMode(*productionFlag || config.RuntimeEnvironmentIndicatesProduction())
	if *rebuildReadModelsFlag {
		if err := security.RequireProxyEncryptionKeyConfigured(); err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := rebuildReadModels(ctx); err != nil {
			return err
		}
		log.Info("Read-model rebuild completed; exiting")
		return nil
	}
	if *migrateOnlyFlag {
		if err := security.RequireProxyEncryptionKeyConfigured(); err != nil {
			return err
		}
		if _, err := database.SetupDB(func(cfg *database.Config) {
			cfg.AutoMigrate = true
		}); err != nil {
			return err
		}
		log.Info("Database migration completed; exiting")
		return nil
	}

	if err := auth.RequireJWTSecretConfigured(); err != nil {
		return err
	}
	if err := auth.RequireJWTTTLConfigured(); err != nil {
		return err
	}
	if err := security.RequireProxyEncryptionKeyConfigured(); err != nil {
		return err
	}
	if err := support.RequireEmailConfigValid(); err != nil {
		return err
	}
	if err := support.RequirePasswordResetConfigValid(); err != nil {
		return err
	}
	if err := support.RequireWorkspaceInvitationConfigValid(); err != nil {
		return err
	}

	backendPort := resolvePort("BACKEND_PORT", "backend-port", *backendPortFlag)
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient, err := support.GetRedisClient()
	if err != nil {
		log.Warn("Redis unavailable at startup; continuing in degraded mode", "error", err)
	}
	pace := installPacedQueues(redisClient)

	heartbeatCancel := runtime.LaunchInstanceHeartbeat(rootCtx, redisClient)
	running, setupErr := bootstrap.Setup(rootCtx, pace)
	defer func() {
		heartbeatCancel()
		if err := proxyqueue.PublicProxyQueue.Close(); err != nil {
			log.Warn("error closing proxy queue", "error", err)
		}
		if err := sitequeue.PublicScrapeSiteQueue.Close(); err != nil {
			log.Warn("error closing scrape-site queue", "error", err)
		}
	}()

	err = serveAfterSetup(rootCtx, backendPort, setupErr, pace)
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), resolveBackgroundShutdownTimeout())
	defer cancel()
	log.Info("Waiting for background jobs and final usage/statistics flushes")
	return errors.Join(err, running.Shutdown(shutdownCtx))
}

// installPacedQueues constructs the process's one Pacer and installs the
// proxy and scrape queues that schedule with it. Nothing starts here:
// bootstrap.Setup configures and runs the Pacer, and the caller passes the same
// Pacer to it and to the API.
func installPacedQueues(redisClient *redis.Client) *checkpace.Pacer {
	pace := checkpace.NewPacer()
	proxyqueue.PublicProxyQueue = *proxyqueue.NewRedisProxyQueue(redisClient, pace)
	sitequeue.PublicScrapeSiteQueue = *sitequeue.NewRedisScrapeSiteQueue(redisClient, pace)
	return pace
}

func serveAfterSetup(ctx context.Context, port int, setupErr error, pace *checkpace.Pacer) error {
	// A signal during optional startup work follows the normal graceful shutdown
	// path. Preserve any separate setup failure, including a joined error.
	if setupErr == context.Canceled && ctx.Err() == context.Canceled {
		return nil
	}
	if setupErr != nil {
		return setupErr
	}
	if err := ctx.Err(); err != nil {
		if err == context.Canceled {
			return nil
		}
		return err
	}
	err := server.OpenRoutes(ctx, port, pace)
	if err == context.Canceled && ctx.Err() == context.Canceled {
		return nil
	}
	return err
}

func resolveBackgroundShutdownTimeout() time.Duration {
	seconds := support.GetEnvInt("BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS", 60)
	if seconds <= 0 {
		seconds = 60
	}
	return time.Duration(seconds) * time.Second
}

func resolvePort(primaryEnv, legacyEnv string, fallback int) int {
	if port := readPort(primaryEnv); port != 0 {
		return port
	}
	if port := readPort(legacyEnv); port != 0 {
		return port
	}
	return fallback
}

func readPort(envKey string) int {
	raw := os.Getenv(envKey)
	if raw == "" {
		return 0
	}
	port, err := strconv.Atoi(raw)
	if err != nil || port == 0 {
		log.Warn("invalid port override", "env", envKey, "value", raw)
		return 0
	}
	return port
}

func configureLogLevel() {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL")))

	switch raw {
	case "", "info":
		log.SetLevel(log.InfoLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "warn", "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	default:
		log.SetLevel(log.InfoLevel)
		log.Warn("invalid LOG_LEVEL value, defaulting to info", "value", raw)
	}
}
