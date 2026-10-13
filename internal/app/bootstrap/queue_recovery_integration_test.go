package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"magpie/internal/config"
)

// Launch the real application so omitting the production monitor caller fails
// this test. Services must be disposable; database 15 of the test Redis must be
// empty. A fresh PostgreSQL database is migrated and removed by this test.
func TestApplicationStartupRecoversLostQueues(t *testing.T) {
	dsn, redisURL := os.Getenv("MAGPIE_TEST_POSTGRES_DSN"), os.Getenv("MAGPIE_TEST_REDIS_URL")
	if dsn == "" || redisURL == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN and MAGPIE_TEST_REDIS_URL are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.ConnectConfig(ctx, pgConfig.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	dbName := fmt.Sprintf("queue_recovery_%d", time.Now().UnixNano())
	dbIdentifier := pgx.Identifier{dbName}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbIdentifier); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanupCtx, "DROP DATABASE "+dbIdentifier+" WITH (FORCE)"); err != nil {
			t.Errorf("remove test database: %v", err)
		}
	}()
	redisOptions, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	redisOptions.DB = 15
	client := redis.NewClient(redisOptions)
	defer client.Close()
	if size, err := client.DBSize(ctx).Result(); err != nil || size != 0 {
		t.Fatalf("live application test requires empty Redis database 15: keys=%d err=%v", size, err)
	}
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := client.FlushDB(cleanupCtx).Err(); err != nil {
			t.Errorf("remove test Redis state: %v", err)
		}
	}()
	directory := t.TempDir()
	backendRoot, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := os.Getenv("MAGPIE_TEST_BACKEND_BINARY")
	if binary == "" {
		binary = filepath.Join(directory, "magpie")
		build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/magpie")
		build.Dir = backendRoot
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build application: %v\n%s", err, output)
		}
	}
	data, err := os.ReadFile(filepath.Join(backendRoot, "internal/config/default_settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var settings config.Config
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatal(err)
	}
	settings.Checker.DynamicThreads, settings.Scraper.DynamicThreads = false, false
	settings.Checker.Threads, settings.Checker.MaxThreads = 1, 1
	settings.Scraper.Threads, settings.Scraper.MaxThreads = 1, 1
	settings.Checker.Judges, settings.Checker.IpLookup = nil, ""
	settings.BlacklistSources = nil
	settings.Plugins.GeoLite.Enabled, settings.Plugins.AbuseIPDB.Enabled = false, false
	data, err = json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "data/settings.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	redisOptions.DB = 15
	dbPassword := pgConfig.Password
	if dbPassword == "" {
		// Trust-authenticated disposable services still need a nonempty value
		// in the application's keyword DSN.
		dbPassword = "isolated-trust-test-password"
	}
	overrides := map[string]string{
		"DB_HOST": pgConfig.Host, "DB_PORT": strconv.Itoa(int(pgConfig.Port)),
		"DB_NAME": dbName, "DB_USERNAME": pgConfig.User, "DB_PASSWORD": dbPassword,
		"DB_SSLMODE": "disable", "DB_AUTO_MIGRATE": "false", "BACKEND_PORT": strconv.Itoa(port),
		"REDIS_URL": fmt.Sprintf("redis://%s/15", redisOptions.Addr), "REDIS_MODE": "single", "REDIS_PASSWORD": redisOptions.Password,
		"JWT_SECRET": "queue-recovery-isolated-test-secret", "PROXY_ENCRYPTION_KEY": "queue-recovery-isolated-test-key",
		"DEVELOPMENT_FIXTURES": "false", "STRICT_SECRET_VALIDATION": "false", "EMAIL_ENABLED": "false",
		"APP_ENV": "development", "ENVIRONMENT": "development", "GO_ENV": "development", "MAGPIE_ENV": "development",
		"ALLOW_PUBLIC_OBSERVABILITY_ENDPOINTS": "true", "STARTUP_QUEUE_BOOTSTRAP_ASYNC": "true",
		"READYZ_ALLOW_REDIS_DEGRADED": "false", "LOG_LEVEL": "info",
	}
	var environment []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if _, replaced := overrides[key]; !replaced {
			environment = append(environment, value)
		}
	}
	for key, value := range overrides {
		environment = append(environment, key+"="+value)
	}
	migrate := exec.CommandContext(ctx, binary, "--migrate-only")
	migrate.Dir, migrate.Env = directory, environment
	if output, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("migrate application fixture: %v\n%s", err, output)
	}
	pgConfig.Database = dbName
	fixture, err := pgx.ConnectConfig(ctx, pgConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close(context.Background())
	const sourceURL = "http://127.0.0.1:1/proxies"
	if _, err := fixture.Exec(ctx, `WITH workspace AS (
		INSERT INTO workspaces (name, created_at, updated_at) VALUES ('Queue recovery fixture', NOW(), NOW()) RETURNING id
	), proxy AS (
		INSERT INTO proxies (host, ip_address, port, country, estimated_type, hash, created_at)
		VALUES ('127.0.0.1', '127.0.0.1', 1, 'Germany', 'Datacenter', decode(repeat('ab',32), 'hex'), NOW()) RETURNING id
	), site AS (
		INSERT INTO scrape_sites (url, created_at) VALUES ($1, NOW()) RETURNING id
	), managed AS (
		INSERT INTO user_proxies (workspace_id, proxy_id, state, created_at, updated_at)
		SELECT workspace.id, proxy.id, 'active', NOW(), NOW() FROM workspace, proxy
	)
	INSERT INTO user_scrape_site (workspace_id, scrape_site_id, fetch_mode, created_at)
	SELECT workspace.id, site.id, 'http', NOW() FROM workspace, site`, sourceURL); err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(directory, "backend.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	process := exec.CommandContext(ctx, binary)
	process.Dir, process.Env = directory, environment
	process.Stdout, process.Stderr = logFile, logFile
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	defer func() {
		process.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil && ctx.Err() == nil {
				t.Errorf("application shutdown: %v", err)
			}
		case <-time.After(8 * time.Second):
			process.Process.Kill()
			<-done
			t.Error("application did not shut down after interrupt")
		}
		if t.Failed() {
			output, _ := os.ReadFile(logFile.Name())
			t.Logf("application output:\n%s", output)
		}
	}()
	httpClient := &http.Client{Timeout: 2 * time.Second}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	ready := func(wantCode int, wantDone bool) bool {
		response, err := httpClient.Get(baseURL + "/readyz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var body struct {
			Done bool `json:"startup_queue_bootstrap_completed"`
		}
		err = json.NewDecoder(response.Body).Decode(&body)
		return err == nil && response.StatusCode == wantCode && body.Done == wantDone
	}
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		for !condition() {
			select {
			case err := <-done:
				done <- err
				t.Fatalf("application exited during %s: %v", label, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-deadline.C:
				t.Fatalf("timed out waiting for %s", label)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	wait("initial readiness", func() bool { return ready(http.StatusOK, true) })
	// Wait for the initial workers to finish so they cannot repopulate keys
	// from an in-flight check after the simulated state loss.
	scheduledQueues := func() bool {
		for _, pattern := range []string{"proxy_queue:*", "scrapesite_queue:*"} {
			keys, err := client.Keys(ctx, pattern).Result()
			if err != nil || len(keys) == 0 {
				return false
			}
			count := 0
			for _, key := range keys {
				members, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
				if err != nil {
					return false
				}
				for _, member := range members {
					count++
					if member.Score != math.Trunc(member.Score) || member.Score < float64(time.Now().Add(time.Minute).UnixMilli()) {
						return false
					}
				}
			}
			if count != 1 {
				return false
			}
		}
		return true
	}
	wait("future scheduling", scheduledQueues)
	for cycle := 1; cycle <= 2; cycle++ {
		// A second instance holds reconstruction leadership. Readiness must
		// become unavailable even though this instance cannot acquire the lock.
		if err := client.Set(ctx, startupQueueBootstrapLockKey, "isolated-test-leader", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		keys := []string{runtimeQueueBootstrapMarker, "proxy_queue", "proxy_queue_heads", "scrapesite_queue", "scrapesite_queue_heads"}
		for _, pattern := range []string{"proxy:*", "proxy_queue:*", "scrapesite:*", "scrapesite_queue:*"} {
			found, err := client.Keys(ctx, pattern).Result()
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, found...)
		}
		if err := client.Del(ctx, keys...).Err(); err != nil {
			t.Fatal(err)
		}
		wait("unavailable readiness during reconstruction", func() bool { return ready(http.StatusServiceUnavailable, false) })
		if marker, err := client.Exists(ctx, runtimeQueueBootstrapMarker).Result(); err != nil || marker != 0 {
			t.Fatalf("reconstruction bypassed held leader lock: marker=%d err=%v", marker, err)
		}
		t.Logf("loss_cycle=%d readiness_during_reconstruction=503 bootstrap_completed=false", cycle)
		if err := client.Del(ctx, startupQueueBootstrapLockKey).Err(); err != nil {
			t.Fatal(err)
		}
		wait("reconstruction and restored readiness", func() bool {
			count, err := client.Exists(ctx, runtimeQueueBootstrapMarker, "proxy:"+strings.Repeat("\xab", 32), "scrapesite:"+sourceURL).Result()
			return err == nil && count == 3 && ready(http.StatusOK, true)
		})
		wait("restored proxy and source scheduling", scheduledQueues)
		t.Logf("loss_cycle=%d readiness_after_reconstruction=200 proxy_and_source_payloads_restored=true scheduled_routes=1 scheduled_sources=1", cycle)
	}
	response, err := httpClient.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	metrics, err := io.ReadAll(response.Body)
	if err != nil || !strings.Contains(string(metrics), `magpie_maintenance_last_success_timestamp_seconds{job="queue_recovery"}`) {
		t.Fatalf("queue recovery success metric missing: %v", err)
	}
}
