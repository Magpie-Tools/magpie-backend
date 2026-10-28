package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const readModelRebuildCompleted = "Read-model rebuild completed; exiting"

// Like the startup/shutdown subprocess fixtures, this runs the shipped entry
// point against an isolated database. The supplied binary can be race-enabled.
func TestApplicationRebuildReadModels(t *testing.T) {
	dsn := os.Getenv("MAGPIE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN enables the isolated CLI regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := newReadModelRebuildFixture(t, ctx, dsn)
	before := fixture.durableFingerprint(t)

	t.Run("requires_stable_key", func(t *testing.T) {
		output, err := fixture.command(t, map[string]string{"PROXY_ENCRYPTION_KEY": ""}, "--rebuild-read-models").CombinedOutput()
		if err == nil || !strings.Contains(string(output), "proxy encryption key not set") {
			t.Fatalf("missing key was accepted: %v\n%s", err, output)
		}
		assertRebuildNotCompleted(t, output)
	})
	t.Run("rejects_migration_combination", func(t *testing.T) {
		output, err := fixture.command(t, nil, "--migrate-only", "--rebuild-read-models").CombinedOutput()
		if err == nil || !strings.Contains(string(output), "cannot be combined") {
			t.Fatalf("combined modes were accepted: %v\n%s", err, output)
		}
		assertRebuildNotCompleted(t, output)
	})
	t.Run("rejects_unknown_flag_without_runtime_setup", func(t *testing.T) {
		output, err := fixture.command(t, nil, "--rebuild-read-models", "--unknown-rebuild-option").CombinedOutput()
		if err == nil || !strings.Contains(string(output), "flag provided but not defined") {
			t.Fatalf("unknown flag was accepted: %v\n%s", err, output)
		}
		assertRebuildNotCompleted(t, output)
		fixture.assertOffline(t)
	})
	t.Run("signal_cancels_database_connection", func(t *testing.T) {
		server, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		accepted := make(chan net.Conn, 1)
		go func() { conn, _ := server.Accept(); accepted <- conn }()
		logFile, err := os.CreateTemp(t.TempDir(), "canceled-connect-*.log")
		if err != nil {
			t.Fatal(err)
		}
		defer logFile.Close()
		process := fixture.command(t, map[string]string{
			"DB_HOST": "127.0.0.1", "DB_PORT": strconv.Itoa(server.Addr().(*net.TCPAddr).Port),
		}, "--rebuild-read-models")
		process.Stdout, process.Stderr = logFile, logFile
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- process.Wait() }()
		waited := false
		defer func() {
			if !waited {
				_ = process.Process.Kill()
				<-done
			}
		}()
		select {
		case conn := <-accepted:
			if conn == nil {
				t.Fatal("connection fixture stopped before accept")
			}
			defer conn.Close()
		case <-time.After(3 * time.Second):
			t.Fatal("repair did not begin the database handshake")
		}
		started := time.Now()
		if err := process.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			waited = true
			if err == nil {
				t.Fatal("canceled database connection exited successfully")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("SIGTERM did not cancel the database handshake")
		}
		output, err := os.ReadFile(logFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		assertRebuildNotCompleted(t, output)
		// SIGTERM may cancel DialContext before it returns the accepted socket,
		// or the subsequent PostgreSQL handshake. Those paths use different
		// cancellation messages; both must identify the database connection.
		connectionError := string(output)
		if !strings.Contains(connectionError, "read-model rebuild database connection:") ||
			(!strings.Contains(connectionError, "context canceled") && !strings.Contains(connectionError, "operation was canceled")) {
			t.Fatalf("process did not report connection cancellation:\n%s", output)
		}
		t.Logf("SIGTERM stopped the database handshake in %s", time.Since(started))
	})
	t.Run("repairs_nonempty_derived_tables_only", func(t *testing.T) {
		fixture.rebuild(t)
		fixture.assertProjection(t, true)
		fixture.assertOffline(t)
		if after := fixture.durableFingerprint(t); !reflect.DeepEqual(before, after) {
			t.Fatal("repair changed durable inventory, credentials, checker settings, history or defaults")
		}
		t.Logf("repaired six owner/route pairs and two sources; %d durable table hashes unchanged; Redis contacts=0", len(before))
	})
	t.Run("failed_projection_exits_nonzero", func(t *testing.T) {
		fixture.staleProjection(t)
		fixture.exec(t, `CREATE FUNCTION reject_rebuild_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'injected rebuild projection failure'; END $$;
CREATE TRIGGER reject_rebuild_projection BEFORE INSERT OR UPDATE ON user_proxy_filter_indexes
FOR EACH ROW EXECUTE FUNCTION reject_rebuild_projection()`)
		defer fixture.exec(t, "DROP TRIGGER reject_rebuild_projection ON user_proxy_filter_indexes")
		output, err := fixture.command(t, nil, "--rebuild-read-models").CombinedOutput()
		if err == nil || !strings.Contains(string(output), "injected rebuild projection failure") {
			t.Fatalf("projection failure was ignored: %v\n%s", err, output)
		}
		assertRebuildNotCompleted(t, output)
		fixture.assertProjection(t, false)
		fixture.assertOffline(t)
	})
	t.Run("signal_cancels_blocked_production_query", func(t *testing.T) {
		fixture.staleProjection(t)
		held, err := fixture.db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Rollback(context.Background())
		var holderPID int
		if err := held.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
			t.Fatal(err)
		}
		if _, err := held.Exec(ctx, "SELECT proxy_id FROM user_proxy_filter_indexes WHERE workspace_id=101 AND proxy_id=201 FOR UPDATE"); err != nil {
			t.Fatal(err)
		}
		logFile, err := os.CreateTemp(t.TempDir(), "canceled-rebuild-*.log")
		if err != nil {
			t.Fatal(err)
		}
		defer logFile.Close()
		process := fixture.command(t, nil, "--rebuild-read-models")
		process.Stdout, process.Stderr = logFile, logFile
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- process.Wait() }()
		waited := false
		defer func() {
			if !waited {
				_ = process.Process.Kill()
				<-done
			}
		}()
		pid := fixture.waitForBlockedProjection(t, holderPID, done)
		started := time.Now()
		if err := process.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			waited = true
			if err == nil {
				t.Fatal("canceled rebuild exited successfully")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("SIGTERM did not cancel the blocked rebuild")
		}
		output, err := os.ReadFile(logFile.Name())
		if err != nil {
			t.Fatal(err)
		}
		assertRebuildNotCompleted(t, output)
		if !strings.Contains(string(output), "context canceled") && !strings.Contains(string(output), "canceling statement") {
			t.Fatalf("process did not report query cancellation:\n%s", output)
		}
		var active bool
		if err := fixture.admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active')", pid).Scan(&active); err != nil {
			t.Fatal(err)
		}
		if active {
			t.Fatal("canceled rebuild left an active PostgreSQL query after process exit")
		}
		if err := held.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		fixture.assertProjection(t, false)
		fixture.assertOffline(t)
		t.Logf("SIGTERM stopped blocked projection and backend query in %s", time.Since(started))
	})
	t.Run("repair_can_resume_after_failure_or_cancellation", func(t *testing.T) {
		fixture.rebuild(t)
		fixture.assertProjection(t, true)
		fixture.assertOffline(t)
		if after := fixture.durableFingerprint(t); !reflect.DeepEqual(before, after) {
			t.Fatal("retry changed durable inventory or credentials")
		}
	})
}

type readModelRebuildFixture struct {
	ctx         context.Context
	db          *pgx.Conn
	admin       *pgx.Conn
	database    string
	binary      string
	directory   string
	environment map[string]string
	redisCalls  *atomic.Int32
}

func newReadModelRebuildFixture(t *testing.T, ctx context.Context, dsn string) *readModelRebuildFixture {
	t.Helper()
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.ConnectConfig(ctx, pgConfig.Copy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	name := fmt.Sprintf("read_model_cli_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+identifier+" WITH (FORCE)"); err != nil {
			t.Errorf("remove rebuild CLI database: %v", err)
		}
	})
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := os.Getenv("MAGPIE_TEST_BACKEND_BINARY")
	if binary == "" {
		binary = filepath.Join(t.TempDir(), "magpie-rebuild-probe")
		build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/magpie")
		build.Dir = root
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build rebuild CLI probe: %v\n%s", err, output)
		}
	}
	api, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Close() })
	redis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int32{}
	redisDone := make(chan struct{})
	go func() {
		defer close(redisDone)
		for {
			conn, err := redis.Accept()
			if err != nil {
				return
			}
			calls.Add(1)
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = redis.Close(); <-redisDone })
	password := pgConfig.Password
	if password == "" {
		password = "isolated-trust-test-password"
	}
	fixture := &readModelRebuildFixture{ctx: ctx, admin: admin, database: name, binary: binary,
		directory: t.TempDir(), redisCalls: calls, environment: map[string]string{
			"DB_HOST": pgConfig.Host, "DB_PORT": strconv.Itoa(int(pgConfig.Port)), "DB_NAME": name,
			"DB_USERNAME": pgConfig.User, "DB_PASSWORD": password, "DB_SSLMODE": "disable", "DB_AUTO_MIGRATE": "true",
			"PGAPPNAME":    name,
			"BACKEND_PORT": strconv.Itoa(api.Addr().(*net.TCPAddr).Port),
			"REDIS_URL":    "redis://" + redis.Addr().String(), "REDIS_MODE": "single", "REDIS_PASSWORD": "",
			"JWT_SECRET": "", "JWT_TTL_MINUTES": "invalid", "PROXY_ENCRYPTION_KEY": "rebuild-cli-isolated-stable-encryption-key-0123456789",
			"STRICT_SECRET_VALIDATION": "false", "DEVELOPMENT_FIXTURES": "true", "EMAIL_ENABLED": "true",
			"APP_ENV": "development", "ENVIRONMENT": "development", "GO_ENV": "development", "MAGPIE_ENV": "development", "LOG_LEVEL": "info",
		}}
	output, err := fixture.command(t, nil, "--migrate-only").CombinedOutput()
	if err != nil || !strings.Contains(string(output), "Database migration completed; exiting") {
		t.Fatalf("migrate-only fixture preparation: %v\n%s", err, output)
	}
	pgConfig.Database = name
	fixture.db, err = pgx.ConnectConfig(ctx, pgConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.db.Close(context.Background()) })
	fixture.exec(t, `
INSERT INTO workspaces (id,name,http_protocol,http_s_protocol,checker_generation,checker_http_key,checker_dirty)
VALUES (101,'Repair owner A',TRUE,FALSE,1,'repair-http-a',FALSE), (102,'Repair owner B',TRUE,FALSE,1,'repair-http-b',FALSE);
INSERT INTO proxies (id,host,ip_address,port,country,estimated_type,hash,created_at)
SELECT n,'192.0.2.10','192.0.2.10',3128,'Germany','Datacenter',decode(repeat(to_hex(n-200),64),'hex'),CURRENT_TIMESTAMP
FROM generate_series(201,203) n;
INSERT INTO user_proxies (workspace_id,proxy_id,username,password,state,pause_reason,created_at,updated_at)
SELECT w.id,p.id,'enc:opaque-user-'||w.id,'enc:opaque-password-'||w.id,
CASE p.id WHEN 202 THEN 'paused' WHEN 203 THEN 'archived' ELSE 'active' END,
CASE WHEN p.id=202 THEN 'manual' ELSE '' END,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM workspaces w CROSS JOIN proxies p;
INSERT INTO judges (id,full_string) VALUES (301,'http://rebuild.invalid/judge');
INSERT INTO proxy_statistics (id,proxy_id,protocol_id,judge_id,alive,attempt,response_time,created_at)
SELECT n+200,n,1,301,TRUE,0,37,CURRENT_TIMESTAMP FROM generate_series(201,203) n;
INSERT INTO proxy_latest_statistics (workspace_id,config_key,transport_protocol,proxy_id,protocol_id,statistic_id,alive,response_time,checked_at)
SELECT w.id,w.checker_http_key,'tcp',p.id,1,p.id+200,w.id=101,37,CURRENT_TIMESTAMP FROM workspaces w CROSS JOIN proxies p;
INSERT INTO scrape_sites (id,url,created_at) VALUES (501,'http://rebuild.invalid/source',CURRENT_TIMESTAMP);
INSERT INTO user_scrape_site (workspace_id,scrape_site_id,fetch_mode,created_at) SELECT id,501,'http',CURRENT_TIMESTAMP FROM workspaces;
INSERT INTO proxy_scrape_site (proxy_id,scrape_site_id) SELECT id,501 FROM proxies;
INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,port,alive,latest_check,updated_at)
SELECT w.id,p.id,'stale.invalid',1,w.id=102,'2000-01-01','2000-01-01' FROM workspaces w CROSS JOIN proxies p;
INSERT INTO user_scrape_source_stats (workspace_id,scrape_site_id,url,proxy_count,alive_count,dead_count)
SELECT id,501,'stale.invalid',999,999,999 FROM workspaces;
INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,port) VALUES (101,299,'orphan.invalid',1);
INSERT INTO user_scrape_source_stats (workspace_id,scrape_site_id,url) VALUES (101,599,'orphan.invalid');
DELETE FROM anonymity_levels;
CREATE FUNCTION reject_rebuild_defaults() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'repair attempted default seeding'; END $$;
CREATE TRIGGER reject_rebuild_defaults BEFORE INSERT ON anonymity_levels FOR EACH STATEMENT EXECUTE FUNCTION reject_rebuild_defaults();
CREATE FUNCTION reject_rebuild_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TAG IN ('CREATE TABLE','ALTER TABLE','DROP TABLE','CREATE INDEX','ALTER INDEX','DROP INDEX') THEN
  RAISE EXCEPTION 'repair attempted schema migration';
 END IF;
END $$;
CREATE EVENT TRIGGER reject_rebuild_ddl ON ddl_command_start EXECUTE FUNCTION reject_rebuild_ddl();`)
	return fixture
}

func (f *readModelRebuildFixture) command(t *testing.T, overrides map[string]string, args ...string) *exec.Cmd {
	t.Helper()
	values := make(map[string]string, len(f.environment)+len(overrides))
	for key, value := range f.environment {
		values[key] = value
	}
	for key, value := range overrides {
		values[key] = value
	}
	var environment []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if _, replaced := values[key]; !replaced {
			environment = append(environment, value)
		}
	}
	for key, value := range values {
		environment = append(environment, key+"="+value)
	}
	cmd := exec.CommandContext(f.ctx, f.binary, args...)
	cmd.Dir, cmd.Env = f.directory, environment
	return cmd
}

func (f *readModelRebuildFixture) exec(t *testing.T, query string) {
	t.Helper()
	if _, err := f.db.Exec(f.ctx, query); err != nil {
		t.Fatal(err)
	}
}

func (f *readModelRebuildFixture) rebuild(t *testing.T) {
	t.Helper()
	output, err := f.command(t, nil, "--rebuild-read-models").CombinedOutput()
	if err != nil || !strings.Contains(string(output), readModelRebuildCompleted) {
		t.Fatalf("rebuild CLI: %v\n%s", err, output)
	}
}

func assertRebuildNotCompleted(t *testing.T, output []byte) {
	t.Helper()
	if strings.Contains(string(output), readModelRebuildCompleted) {
		t.Fatalf("incomplete rebuild logged success:\n%s", output)
	}
}

func (f *readModelRebuildFixture) staleProjection(t *testing.T) {
	t.Helper()
	f.exec(t, "UPDATE user_proxy_filter_indexes SET host='stale.invalid', alive=workspace_id=102, latest_check='2000-01-01'; UPDATE user_scrape_source_stats SET proxy_count=999,alive_count=999,dead_count=999")
}

func (f *readModelRebuildFixture) assertProjection(t *testing.T, repaired bool) {
	t.Helper()
	var rows, correct int
	query := `SELECT COUNT(*),COUNT(*) FILTER (WHERE host='192.0.2.10' AND port=3128 AND alive=(workspace_id=101)
 AND response_time=37 AND latest_check=(SELECT MIN(checked_at) FROM proxy_latest_statistics)
 AND state=CASE proxy_id WHEN 202 THEN 'paused' WHEN 203 THEN 'archived' ELSE 'active' END)
FROM user_proxy_filter_indexes`
	if err := f.db.QueryRow(f.ctx, query).Scan(&rows, &correct); err != nil {
		t.Fatal(err)
	}
	if rows != 6 || (repaired && correct != 6) || (!repaired && correct != 0) {
		t.Fatalf("projection rows=%d repaired=%d want_repaired=%t", rows, correct, repaired)
	}
	if repaired {
		if err := f.db.QueryRow(f.ctx, "SELECT COUNT(*) FROM user_scrape_source_stats").Scan(&rows); err != nil || rows != 2 {
			t.Fatalf("source projection rows=%d want=2 err=%v", rows, err)
		}
		if err := f.db.QueryRow(f.ctx, `SELECT COUNT(*) FROM user_scrape_source_stats WHERE proxy_count=3
AND alive_count=CASE WHEN workspace_id=101 THEN 3 ELSE 0 END AND dead_count=CASE WHEN workspace_id=102 THEN 3 ELSE 0 END AND unknown_count=0`).Scan(&correct); err != nil || correct != 2 {
			t.Fatalf("source projections correct=%d want=2 err=%v", correct, err)
		}
	}
}

func (f *readModelRebuildFixture) durableFingerprint(t *testing.T) map[string]string {
	t.Helper()
	rows, err := f.db.Query(f.ctx, "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('user_proxy_filter_indexes','user_scrape_source_stats') ORDER BY tablename")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string, len(names))
	for _, name := range names {
		var payload string
		if err := f.db.QueryRow(f.ctx, "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM "+pgx.Identifier{name}.Sanitize()+" t").Scan(&payload); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(payload))
		result[name] = hex.EncodeToString(digest[:])
	}
	return result
}

func (f *readModelRebuildFixture) assertOffline(t *testing.T) {
	t.Helper()
	if f.redisCalls.Load() != 0 {
		t.Fatalf("database-only repair contacted Redis %d times", f.redisCalls.Load())
	}
	for _, name := range []string{"data/jwt_secret", "data/settings.json"} {
		if _, err := os.Stat(filepath.Join(f.directory, name)); !os.IsNotExist(err) {
			t.Fatalf("database-only repair initialized %s: %v", name, err)
		}
	}
}

func (f *readModelRebuildFixture) waitForBlockedProjection(t *testing.T, holderPID int, done chan error) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var pid int
		if err := f.admin.QueryRow(f.ctx, `SELECT COALESCE((SELECT pid FROM pg_stat_activity WHERE datname=$1 AND application_name=$3
AND $2=ANY(pg_blocking_pids(pid)) AND query LIKE '%WITH scope AS MATERIALIZED%' LIMIT 1),0)`, f.database, holderPID, f.database).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		if pid != 0 {
			return pid
		}
		select {
		case err := <-done:
			done <- err
			t.Fatalf("rebuild exited before the projection lock: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("rebuild did not reach the held production projection query")
	return 0
}
