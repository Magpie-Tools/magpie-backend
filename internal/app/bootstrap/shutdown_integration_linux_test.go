package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
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

// The overlaid main injects work, then sends SIGTERM to the real app.Run.
// The read-model regression also registers a draining producer through a setup
// overlay; the application's shutdown implementation runs unchanged.
const shutdownProbeMain = `package main
import (
 "fmt"
 "context"
 "magpie/internal/app"
 "magpie/internal/database"
 "magpie/internal/domain"
 jobs "magpie/internal/jobs/runtime"
 "net/http"
 "os"
 "syscall"
 "time"
)
func main() {
 go func() {
  client := &http.Client{Timeout: time.Second}
  for {
   response, err := client.Get("http://127.0.0.1:"+os.Getenv("BACKEND_PORT")+"/readyz")
   if err == nil { response.Body.Close(); if response.StatusCode == 200 { break } }
   time.Sleep(50*time.Millisecond)
  }
  workspace := domain.Workspace{Name: "Shutdown probe", HTTPProtocol: true}
  proxy := domain.Proxy{IP: "8.8.8.8", Port: 8080}
  judge := domain.Judge{FullString: "http://shutdown.invalid/judge"}
  protocol := domain.Protocol{}
  for _, err := range []error{database.DB.Create(&workspace).Error, database.DB.Create(&proxy).Error, database.DB.Create(&judge).Error, database.DB.First(&protocol).Error} {
   if err != nil { panic(err) }
  }
  if err := database.DB.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID}).Error; err != nil { panic(err) }
  for i := 0; i < 3; i++ {
   site := domain.ScrapeSite{URL: fmt.Sprintf("http://shutdown.invalid/source/%d", i)}
   if err := database.DB.Create(&site).Error; err != nil { panic(err) }
   if err := database.DB.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil { panic(err) }
   if err := database.DB.Create(&domain.ProxyScrapeSite{ProxyID: proxy.ID, ScrapeSiteID: site.ID}).Error; err != nil { panic(err) }
  }
  snapshot, err := database.LoadCheckerWorkspace(context.Background(), workspace.ID)
  if err != nil { panic(err) }
  if err := database.RefreshReadModelIndexes(context.Background()); err != nil { panic(err) }
  jobs.AddWorkspaceManagedTraffic(workspace.ID, 1, 123)
  jobs.AddProxyStatistic(domain.ProxyStatistic{CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: workspace.ID, ConfigKey: snapshot.Default.Keys[protocol.ID-1], Alive: true}}, Alive: true, Attempt: 1, ResponseTime: 10, TransportProtocol: "tcp", ProxyID: proxy.ID, JudgeID: judge.ID, ProtocolID: protocol.ID, CreatedAt: time.Now().UTC()})
  fmt.Println("SHUTDOWN_PROBE submitted usage and statistic")
  _ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
 }()
 if err := app.Run(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
`

func TestApplicationSIGTERMFlushesUsageAndMemoryStatistics(t *testing.T) {
	testApplicationSIGTERM(t, false)
}

func TestApplicationSIGTERMDrainsReadModelsAfterReputation(t *testing.T) {
	testApplicationSIGTERM(t, true)
}

func testApplicationSIGTERM(t *testing.T, drainReputation bool) {
	t.Helper()
	dsn, redisURL := os.Getenv("MAGPIE_TEST_POSTGRES_DSN"), os.Getenv("MAGPIE_TEST_REDIS_URL")
	if dsn == "" || redisURL == "" {
		t.Skip("set disposable PostgreSQL and Redis test services")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	buildDir := t.TempDir()
	mainFile, overlayFile := filepath.Join(buildDir, "main.go"), filepath.Join(buildDir, "overlay.json")
	if err := os.WriteFile(mainFile, []byte(shutdownProbeMain), 0600); err != nil {
		t.Fatal(err)
	}
	replacements := map[string]string{filepath.Join(root, "cmd/magpie/main.go"): mainFile}
	if drainReputation {
		// Model a slow producer draining in-flight work. Let statistics and
		// reputation commit and delete their recovery jobs before producers
		// finish. Only the read-model cancellation flush can update the filters
		// and the scrape-source counters.
		setupPath := filepath.Join(root, "internal/app/bootstrap/setup.go")
		setup, err := os.ReadFile(setupPath)
		if err != nil {
			t.Fatal(err)
		}
		const marker = "\t// Routines"
		if strings.Count(string(setup), marker) != 1 {
			t.Fatal("cannot locate routine registration for shutdown fixture")
		}
		const producer = `
	running.producers.start(func(ctx context.Context) {
		<-ctx.Done()
		drainCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		for {
			var complete bool
			err := database.DB.WithContext(drainCtx).Raw("SELECT EXISTS (SELECT 1 FROM proxy_latest_statistics WHERE workspace_id > 0 AND alive) AND NOT EXISTS (SELECT 1 FROM proxy_reputation_refreshes)").Scan(&complete).Error
			if err != nil { panic(err) }
			if complete { return }
			select {
			case <-drainCtx.Done(): panic("shutdown fixture timed out waiting for reputation")
			case <-time.After(50*time.Millisecond):
			}
		}
	})
`
		setupFile := filepath.Join(buildDir, "setup.go")
		if err := os.WriteFile(setupFile, []byte(strings.Replace(string(setup), marker, marker+producer, 1)), 0600); err != nil {
			t.Fatal(err)
		}
		replacements[setupPath] = setupFile
	}
	overlay, _ := json.Marshal(map[string]any{"Replace": replacements})
	if err := os.WriteFile(overlayFile, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(buildDir, "shutdown-probe")
	args := []string{"build", "-overlay", overlayFile, "-o", binary}
	if reference := os.Getenv("MAGPIE_TEST_BACKEND_BINARY"); reference != "" {
		info, err := exec.Command("go", "version", "-m", reference).CombinedOutput()
		if err != nil {
			t.Fatalf("inspect reference binary: %v %s", err, info)
		}
		if strings.Contains(string(info), "-race=true") {
			args = append(args, "-race")
		}
	}
	build := exec.CommandContext(ctx, "go", append(args, "./cmd/magpie")...)
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build probe: %v\n%s", err, output)
	}
	for _, stream := range []string{"true", "false"} {
		t.Run("redis_stream_"+stream, func(t *testing.T) {
			pgConfig, err := pgx.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			admin, err := pgx.ConnectConfig(ctx, pgConfig.Copy())
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close(context.Background())
			name := fmt.Sprintf("shutdown_probe_%d", time.Now().UnixNano())
			identifier := pgx.Identifier{name}.Sanitize()
			if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_, err := admin.Exec(context.Background(), "DROP DATABASE "+identifier+" WITH (FORCE)")
				if err != nil {
					t.Error(err)
				}
			}()
			options, err := redis.ParseURL(redisURL)
			if err != nil {
				t.Fatal(err)
			}
			options.DB = 14
			redisClient := redis.NewClient(options)
			defer redisClient.Close()
			if size, err := redisClient.DBSize(ctx).Result(); err != nil || size != 0 {
				t.Fatalf("probe requires empty Redis DB 14: %d %v", size, err)
			}
			defer redisClient.FlushDB(context.Background())
			dir := t.TempDir()
			data, err := os.ReadFile(filepath.Join(root, "internal/config/default_settings.json"))
			if err != nil {
				t.Fatal(err)
			}
			var settings config.Config
			if err := json.Unmarshal(data, &settings); err != nil {
				t.Fatal(err)
			}
			settings.Checker.DynamicThreads, settings.Scraper.DynamicThreads = false, false
			settings.Checker.Threads, settings.Scraper.Threads = 0, 0
			settings.Checker.Judges, settings.Checker.IpLookup, settings.BlacklistSources = nil, "", nil
			settings.Plugins.GeoLite.Enabled, settings.Plugins.AbuseIPDB.Enabled = false, false
			data, _ = json.Marshal(settings)
			if err := os.Mkdir(filepath.Join(dir, "data"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "data/settings.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			listener.Close()
			overrides := map[string]string{
				"DB_HOST": pgConfig.Host, "DB_PORT": strconv.Itoa(int(pgConfig.Port)), "DB_NAME": name, "DB_USERNAME": pgConfig.User,
				"DB_PASSWORD": pgConfig.Password, "DB_SSLMODE": "disable", "DB_AUTO_MIGRATE": "false", "BACKEND_PORT": strconv.Itoa(port),
				"REDIS_URL": fmt.Sprintf("redis://%s/14", options.Addr), "REDIS_MODE": "single", "REDIS_PASSWORD": options.Password,
				"JWT_SECRET": "shutdown-probe-isolated-secret", "PROXY_ENCRYPTION_KEY": "shutdown-probe-isolated-key",
				"STRICT_SECRET_VALIDATION": "false", "DEVELOPMENT_FIXTURES": "false", "EMAIL_ENABLED": "false",
				"APP_ENV": "development", "ENVIRONMENT": "development", "GO_ENV": "development", "MAGPIE_ENV": "development",
				"ALLOW_PUBLIC_OBSERVABILITY_ENDPOINTS": "true", "PROXY_STATISTICS_REDIS_STREAM_ENABLED": stream, "LOG_LEVEL": "info",
			}
			if drainReputation {
				// Force both indexes to rely on the final drain, not a timer.
				overrides["READ_MODEL_REFRESH_INTERVAL_SECONDS"] = "3600"
				overrides["SCRAPE_SOURCE_STATS_REFRESH_INTERVAL_SECONDS"] = "3600"
				overrides["SCRAPE_SOURCE_STATS_REFRESH_BATCH_SIZE"] = "1"
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
			migrate.Dir, migrate.Env = dir, environment
			if output, err := migrate.CombinedOutput(); err != nil {
				t.Fatalf("migrate: %v\n%s", err, output)
			}
			pgConfig.Database = name
			db, err := pgx.ConnectConfig(ctx, pgConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close(context.Background())
			if _, err := db.Exec(ctx, `CREATE FUNCTION delay_shutdown_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2); RETURN NEW; END $$;
CREATE TRIGGER delay_usage BEFORE INSERT ON workspace_usage_periods FOR EACH ROW EXECUTE FUNCTION delay_shutdown_insert();
CREATE TRIGGER delay_statistics BEFORE INSERT ON proxy_statistics FOR EACH ROW EXECUTE FUNCTION delay_shutdown_insert();`); err != nil {
				t.Fatal(err)
			}
			probe := exec.CommandContext(ctx, binary)
			probe.Dir, probe.Env = dir, environment
			output, err := probe.CombinedOutput()
			if err != nil {
				t.Fatalf("SIGTERM process: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "SHUTDOWN_PROBE submitted") {
				t.Fatalf("work was not submitted:\n%s", output)
			}
			var requests, bytes, statistics int64
			if err := db.QueryRow(ctx, "SELECT COALESCE(SUM(managed_requests),0), COALESCE(SUM(managed_bytes),0) FROM workspace_usage_periods").Scan(&requests, &bytes); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM proxy_statistics").Scan(&statistics); err != nil {
				t.Fatal(err)
			}
			if requests != 1 || bytes != 123 || (stream == "false" && statistics != 1) {
				t.Fatalf("shutdown lost work: requests=%d bytes=%d memory_statistics=%d\n%s", requests, bytes, statistics, output)
			}
			if drainReputation {
				var alive, indexed bool
				if err := db.QueryRow(ctx, "SELECT alive FROM proxy_latest_statistics WHERE workspace_id > 0").Scan(&alive); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(ctx, "SELECT alive FROM user_proxy_filter_indexes").Scan(&indexed); err != nil {
					t.Fatal(err)
				}
				var pending int64
				if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM proxy_reputation_refreshes").Scan(&pending); err != nil {
					t.Fatal(err)
				}
				if !alive || !indexed || pending != 0 {
					t.Fatalf("shutdown left stale filters: alive=%v indexed=%v pending_reputation=%d\n%s", alive, indexed, pending, output)
				}
				var sources, proxies, sourceAlive, sourceDead, sourceUnknown int64
				if err := db.QueryRow(ctx, "SELECT COUNT(*), COALESCE(SUM(proxy_count),0), COALESCE(SUM(alive_count),0), COALESCE(SUM(dead_count),0), COALESCE(SUM(unknown_count),0) FROM user_scrape_source_stats").Scan(&sources, &proxies, &sourceAlive, &sourceDead, &sourceUnknown); err != nil {
					t.Fatal(err)
				}
				if sources != 3 || proxies != 3 || sourceAlive != 3 || sourceDead != 0 || sourceUnknown != 0 {
					t.Fatalf("shutdown left stale source counters: sources=%d proxies=%d alive=%d dead=%d unknown=%d pending_reputation=%d\n%s", sources, proxies, sourceAlive, sourceDead, sourceUnknown, pending, output)
				}
				t.Logf("SIGTERM drained all source batches: sources=%d alive=%d unknown=%d", sources, sourceAlive, sourceUnknown)
				t.Logf("SIGTERM drained read models after reputation: alive=%v indexed=%v pending=%d", alive, indexed, pending)
			}
			t.Logf("SIGTERM persisted requests=%d bytes=%d statistics=%d with delayed inserts", requests, bytes, statistics)
		})
	}
}
