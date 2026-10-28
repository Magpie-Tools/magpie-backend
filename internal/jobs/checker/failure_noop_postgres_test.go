package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"magpie/internal/database"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type failureResetSQLCapture struct {
	logger.Interface
	statements []string
}

func (c *failureResetSQLCapture) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	if strings.Contains(sql, "UPDATE user_proxies") {
		c.statements = append(c.statements, sql)
	}
}

func TestFailureResetAvoidsZeroWritesAndLocksPostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("MAGPIE_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN is not set")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, _ := admin.DB()
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := fmt.Sprintf("failure_noop_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	previous := database.DB
	database.DB = db
	t.Cleanup(func() { database.DB = previous })
	for _, sql := range []string{
		`CREATE TABLE user_proxies (workspace_id bigint NOT NULL,proxy_id bigint NOT NULL,
consecutive_failures integer NOT NULL DEFAULT 0,username text,password text,state text,
updated_at timestamptz,PRIMARY KEY(workspace_id,proxy_id))`,
		`CREATE INDEX idx_user_proxies_proxy_id ON user_proxies (proxy_id)`,
		`CREATE INDEX idx_managed_proxies_workspace_state ON user_proxies (workspace_id,state)`,
		`INSERT INTO user_proxies SELECT w,p,0,'unchanged-user','unchanged-password','active','2000-01-01'
FROM generate_series(1,8) w CROSS JOIN generate_series(1,10000) p`,
		`UPDATE user_proxies SET consecutive_failures=3 WHERE workspace_id=2 AND proxy_id=1`,
		`UPDATE user_proxies SET consecutive_failures=7 WHERE workspace_id=8 AND proxy_id=2`,
		`CREATE TABLE observed_failure_updates (workspace_id bigint,proxy_id bigint)`,
		`CREATE FUNCTION observe_failure_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
INSERT INTO observed_failure_updates VALUES(NEW.workspace_id,NEW.proxy_id); RETURN NEW; END $$`,
		`CREATE TRIGGER observe_failure_update AFTER UPDATE ON user_proxies FOR EACH ROW EXECUTE FUNCTION observe_failure_update()`,
		`ANALYZE user_proxies`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	pairs := make([]failurePair, 0, failureSQLChunkSize)
	for owner := uint(1); owner <= 8; owner++ {
		for route := 0; route < 25; route++ {
			pairs = append(pairs, failurePair{workspaceID: owner, proxyID: uint64(1 + 9999*route/24)})
		}
	}
	locked := db.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	t.Cleanup(func() { _ = locked.Rollback().Error })
	if err := locked.Exec("SELECT proxy_id FROM user_proxies WHERE workspace_id=1 AND proxy_id=1 FOR UPDATE").Error; err != nil {
		t.Fatal(err)
	}
	capture := &failureResetSQLCapture{Interface: db.Logger}
	database.DB = db.Session(&gorm.Session{Logger: capture})
	done := make(chan error, 1)
	go func() { done <- execResetChunk(pairs) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		_ = locked.Rollback().Error
		<-done
		t.Fatal("zero reset waited for an unrelated ownership row lock")
	}
	_ = locked.Rollback().Error
	if len(capture.statements) != 1 {
		t.Fatal("reset added database statements", capture.statements)
	}
	guardedSQL := capture.statements[0]
	if err := execResetChunk(pairs); err != nil {
		t.Fatal(err)
	}
	var changed, mutations int64
	if err := db.Raw("SELECT count(*) FROM observed_failure_updates").Scan(&mutations).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("SELECT count(*) FROM observed_failure_updates WHERE workspace_id=2 AND proxy_id=1").Scan(&changed).Error; err != nil {
		t.Fatal(err)
	}
	if mutations != 1 || changed != 1 {
		t.Fatal("reset rewrote zero streaks", mutations, changed)
	}
	var altered int64
	if err := db.Raw(`SELECT count(*) FROM user_proxies WHERE username<>'unchanged-user' OR password<>'unchanged-password'
OR state<>'active' OR updated_at<>'2000-01-01'::timestamptz`).Scan(&altered).Error; err != nil {
		t.Fatal(err)
	}
	if altered != 0 {
		t.Fatal("reset changed credentials, lifecycle or mutation timestamps", altered)
	}
	var untouched int
	if err := db.Raw("SELECT consecutive_failures FROM user_proxies WHERE workspace_id=8 AND proxy_id=2").Scan(&untouched).Error; err != nil || untouched != 7 {
		t.Fatal("reset escaped its requested owner/route pairs", untouched, err)
	}
	if err := db.Exec("DROP TRIGGER observe_failure_update ON user_proxies").Error; err != nil {
		t.Fatal(err)
	}
	legacySQL := strings.TrimSuffix(guardedSQL, " AND up.consecutive_failures <> 0")
	if legacySQL == guardedSQL {
		t.Fatal("guard was not captured")
	}
	results := map[string][]map[string]any{}
	for _, phase := range []struct{ name, sql string }{{"original", legacySQL}, {"guarded", guardedSQL}} {
		for trial := 0; trial < 3; trial++ {
			var encoded string
			if err := db.Raw("EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON) " + phase.sql).Row().Scan(&encoded); err != nil {
				t.Fatal(err)
			}
			var plan []map[string]any
			if err := json.Unmarshal([]byte(encoded), &plan); err != nil {
				t.Fatal(err)
			}
			results[phase.name] = append(results[phase.name], plan[0])
			if directory := os.Getenv("MAGPIE_TEST_WRITE_GUARD_EVIDENCE_DIR"); directory != "" {
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, fmt.Sprintf("failure-%s-%d-plan.json", phase.name, trial)), []byte(encoded), 0600); err != nil {
					t.Fatal(err)
				}
			}
			root := plan[0]["Plan"].(map[string]any)
			t.Logf("%s owner_rows=200 inventory=80000 trial=%d execution_ms=%v WAL_records=%v WAL_bytes=%v", phase.name, trial, plan[0]["Execution Time"], root["WAL Records"], root["WAL Bytes"])
		}
	}
	var original, guarded float64
	for _, phase := range []string{"original", "guarded"} {
		for _, sample := range results[phase] {
			wal, _ := sample["Plan"].(map[string]any)["WAL Records"].(float64)
			if phase == "original" {
				original += wal
			} else {
				guarded += wal
			}
		}
	}
	if original == 0 || guarded >= original {
		t.Fatal("guard did not reduce measured WAL for zero resets", original, guarded)
	}
}
