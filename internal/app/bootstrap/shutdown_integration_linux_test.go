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
// Application startup/shutdown code is unchanged by the overlay.
const shutdownProbeMain = `package main
import (
 "fmt"
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
  workspace := domain.Workspace{Name: "Shutdown probe"}
  proxy := domain.Proxy{IP: "8.8.8.8", Port: 8080}
  judge := domain.Judge{FullString: "http://shutdown.invalid/judge"}
  protocol := domain.Protocol{}
  for _, err := range []error{database.DB.Create(&workspace).Error, database.DB.Create(&proxy).Error, database.DB.Create(&judge).Error, database.DB.First(&protocol).Error} {
   if err != nil { panic(err) }
  }
  jobs.AddWorkspaceManagedTraffic(workspace.ID, 1, 123)
  jobs.AddProxyStatistic(domain.ProxyStatistic{Alive: true, Attempt: 1, ResponseTime: 10, TransportProtocol: "tcp", ProxyID: proxy.ID, JudgeID: judge.ID, ProtocolID: protocol.ID, CreatedAt: time.Now().UTC()})
  fmt.Println("SHUTDOWN_PROBE submitted usage and statistic")
  _ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
 }()
 if err := app.Run(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
`

func TestApplicationSIGTERMFlushesUsageAndMemoryStatistics(t *testing.T) {
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
	overlay, _ := json.Marshal(map[string]any{"Replace": map[string]string{filepath.Join(root, "cmd/magpie/main.go"): mainFile}})
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
			t.Logf("SIGTERM persisted requests=%d bytes=%d statistics=%d with delayed inserts", requests, bytes, statistics)
		})
	}
}
