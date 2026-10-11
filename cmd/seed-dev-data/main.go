package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"magpie/internal/config"
	"magpie/internal/database"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
	jobruntime "magpie/internal/jobs/runtime"
	"magpie/internal/security"
	"magpie/internal/support"

	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load .env: %w", err)
	}
	// Queue packages are also used by the API and initialise before main. Reopen
	// their client after dotenv so this command uses the configured Redis.
	if err := support.CloseRedisClient(); err != nil {
		return err
	}
	accounts := flag.String("accounts", "username@gmx.at,username2@gmx.at,username3@gmx.at", "Exactly three development account emails, in persona order")
	proxies := flag.Int("proxies-per-workspace", 1000, "Routes in each of five workspaces, between 10 and 10000")
	days := flag.Int("history-days", 30, "Days of synthetic check history, between 1 and 90")
	seed := flag.Int64("seed", 20261010, "Deterministic fixture seed")
	reset := flag.Bool("reset", false, "Replace the selected accounts' workspaces and data")
	dryRun := flag.Bool("dry-run", false, "Print the reset scope without changing the database")
	backendURL := flag.String("backend-url", "http://127.0.0.1:"+support.GetEnv("BACKEND_PORT", "5656"), "Backend URL used to verify it is stopped")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	options := database.DevelopmentSeedOptions{Emails: strings.Split(*accounts, ","), Password: os.Getenv("MAGPIE_DEV_PASSWORD"),
		ProxiesPerWorkspace: *proxies, HistoryDays: *days, Seed: *seed}
	if config.RuntimeEnvironmentIndicatesProduction() {
		return fmt.Errorf("development data cannot be seeded in production")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	db, err := database.SetupDB(func(cfg *database.Config) {
		cfg.AutoMigrate = false
		cfg.SeedDefaults = false
	})
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	plan, err := database.PlanDevelopmentSeed(ctx, db, options)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if !*reset || *dryRun {
		return encoder.Encode(plan)
	}
	if err := security.RequireProxyEncryptionKeyConfigured(); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(*backendURL, "/")+"/healthz", nil)
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	if err == nil {
		response.Body.Close()
		return fmt.Errorf("stop the backend before resetting development data")
	}
	client, err := support.GetRedisClient()
	if err != nil {
		return err
	}
	defer support.CloseRedisClient()
	instances, err := jobruntime.CountActiveInstances(ctx, client)
	if err != nil {
		return err
	}
	if instances != 0 {
		return fmt.Errorf("stop all backend instances before resetting development data; %d instance heartbeats remain", instances)
	}
	if err := config.ReadSettings(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Replacing %d workspaces and %d routes; generating %d fixtures with %d days of history.\n", len(plan.ResetWorkspaces), plan.ExistingRoutes, plan.NewRoutes, *days)
	result, err := database.SeedDevelopmentData(ctx, db, options)
	if err != nil {
		return fmt.Errorf("development reset rolled back: %w", err)
	}
	// Reconcile only affected runtime entries. Never flush a shared Redis DB.
	queue := proxyqueue.NewRedisProxyQueue(client)
	if err := queue.RemoveFromQueue(result.RemovedProxies); err != nil {
		return fmt.Errorf("fixtures committed but old proxy queue cleanup failed: %w", err)
	}
	if err := queue.AddToQueue(result.RefreshProxies); err != nil {
		return fmt.Errorf("fixtures committed but shared proxy queue refresh failed: %w", err)
	}
	sites := sitequeue.NewRedisScrapeSiteQueue(client)
	if err := sites.RemoveFromQueue(result.RemovedSources); err != nil {
		return fmt.Errorf("fixtures committed but old source queue cleanup failed: %w", err)
	}
	fmt.Fprintln(os.Stderr, "Start the backend with DEVELOPMENT_FIXTURES=true to preserve the simulated data.")
	return encoder.Encode(result)
}
