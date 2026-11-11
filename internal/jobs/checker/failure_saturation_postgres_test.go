package checker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"magpie/internal/platform/pg"
)

type failureIncrementSQLCapture struct {
	logger.Interface
	statements []string
}

func (c *failureIncrementSQLCapture) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	if sql, _ := fc(); strings.Contains(sql, "user_proxies") {
		c.statements = append(c.statements, sql)
	}
}

var errRollbackMeasurement = errors.New("rollback measurement")

// Dead routes shared by many owners must not create row versions once their
// streak saturates. The increment stays one statement, and a failing route
// below saturation costs the same as before.
func TestSaturatedFailureStreaksSkipWritesPostgres(t *testing.T) {
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
	schema := fmt.Sprintf("failure_saturation_%d", time.Now().UnixNano())
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
	previous := pg.DB
	t.Cleanup(func() { pg.DB = previous })

	// 1,000 owners share 50 routes; route 1 is dead for 200 of them. Rows carry
	// ciphertext-sized credentials as in production.
	for _, sql := range []string{
		`CREATE TABLE user_proxies (workspace_id bigint NOT NULL,proxy_id bigint NOT NULL,
consecutive_failures integer NOT NULL DEFAULT 0,username text,password text,state text,
updated_at timestamptz,PRIMARY KEY(workspace_id,proxy_id))`,
		`CREATE INDEX idx_user_proxies_proxy_id ON user_proxies (proxy_id)`,
		`CREATE INDEX idx_managed_proxies_workspace_state ON user_proxies (workspace_id,state)`,
		`INSERT INTO user_proxies SELECT w,p,0,repeat('u',120),repeat('p',120),'active','2000-01-01'
FROM generate_series(1,1000) w CROSS JOIN generate_series(1,50) p`,
		fmt.Sprintf(`UPDATE user_proxies SET consecutive_failures=%d WHERE proxy_id=1 AND workspace_id<=200`, failureStreakSaturation),
		`UPDATE user_proxies SET consecutive_failures=65535 WHERE proxy_id=1 AND workspace_id=3`,
		fmt.Sprintf(`UPDATE user_proxies SET consecutive_failures=%d WHERE proxy_id=2 AND workspace_id=1`, failureStreakSaturation-1),
		`VACUUM ANALYZE user_proxies`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}

	saturated := make([]failureIncrementEntry, 0, failureSQLChunkSize)
	for owner := uint(1); owner <= failureSQLChunkSize; owner++ {
		saturated = append(saturated, failureIncrementEntry{workspaceID: owner, proxyID: 1})
	}
	capture := &failureIncrementSQLCapture{Interface: db.Logger}
	pg.DB = db.Session(&gorm.Session{Logger: capture})
	counts, err := execIncrementChunk(saturated)
	if err != nil {
		t.Fatal(err)
	}
	if len(capture.statements) != 1 {
		t.Fatal("increment must remain one statement per chunk", capture.statements)
	}
	if len(counts) != 0 {
		t.Fatal("saturated streaks were rewritten", counts)
	}
	for _, entry := range saturated[:3] {
		if count := failureCount(counts, entry); count < failureStreakSaturation {
			t.Fatal("an unreturned saturated streak must reach every threshold", count)
		}
	}
	var changed int64
	if err := db.Raw(fmt.Sprintf("SELECT count(*) FROM user_proxies WHERE proxy_id=1 AND workspace_id<=200 AND consecutive_failures NOT IN (%d,65535)", failureStreakSaturation)).Scan(&changed).Error; err != nil || changed != 0 {
		t.Fatal("saturated streak changed", changed, err)
	}

	// Below saturation still increments and stops exactly at the cap.
	pg.DB = db
	below := []failureIncrementEntry{{workspaceID: 1, proxyID: 2}, {workspaceID: 2, proxyID: 2}}
	for round := 0; round < 2; round++ {
		counts, err := execIncrementChunk(below)
		if err != nil {
			t.Fatal(err)
		}
		if round == 0 && (len(counts) != 2 || counts[failureKey{1, 2}] != failureStreakSaturation || counts[failureKey{2, 2}] != 1) {
			t.Fatal("increment below saturation", counts)
		}
		if round == 1 && (len(counts) != 1 || counts[failureKey{2, 2}] != 2) {
			t.Fatal("saturated streak was rewritten", counts)
		}
	}

	// Compare against the previous unconditional LEAST(..., 65535) update for
	// the dead route and for a failing route below saturation.
	failing := make([]failureIncrementEntry, 0, failureSQLChunkSize)
	for owner := uint(301); owner < 301+failureSQLChunkSize; owner++ {
		failing = append(failing, failureIncrementEntry{workspaceID: owner, proxyID: 3, autoRemove: true, threshold: 3})
	}
	type sample struct{ ms, walRecords, walBytes float64 }
	measure := func(fixture string, entries []failureIncrementEntry) map[string]sample {
		capture.statements = nil
		pg.DB = db.Session(&gorm.Session{Logger: capture})
		err := db.Transaction(func(tx *gorm.DB) error {
			pg.DB = tx.Session(&gorm.Session{Logger: capture})
			if _, err := execIncrementChunk(entries); err != nil {
				return err
			}
			return errRollbackMeasurement
		})
		pg.DB = db
		if !errors.Is(err, errRollbackMeasurement) || len(capture.statements) != 1 {
			t.Fatal(err, capture.statements)
		}
		var legacy strings.Builder
		legacy.WriteString("WITH updates(workspace_id, proxy_id, inc) AS (VALUES ")
		for i, entry := range entries {
			if i > 0 {
				legacy.WriteString(",")
			}
			fmt.Fprintf(&legacy, "(%d::bigint,%d::bigint,1::integer)", entry.workspaceID, entry.proxyID)
		}
		legacy.WriteString(")\nUPDATE user_proxies up SET consecutive_failures = LEAST(up.consecutive_failures + updates.inc, 65535) FROM updates WHERE up.workspace_id = updates.workspace_id AND up.proxy_id = updates.proxy_id RETURNING up.workspace_id, up.proxy_id, up.consecutive_failures")
		phases := []struct{ name, sql string }{{"legacy", legacy.String()}, {"saturating", capture.statements[0]}}
		samples := map[string][]sample{}
		// Interleave phases so neither pays for the other's rolled-back
		// row versions more often.
		for trial := 0; trial < 11; trial++ {
			for offset := range phases {
				phase := phases[(trial+offset)%len(phases)]
				var encoded string
				// Roll back so every trial starts from the same fixture.
				err := db.Transaction(func(tx *gorm.DB) error {
					if err := tx.Raw("EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON) " + phase.sql).Row().Scan(&encoded); err != nil {
						return err
					}
					return errRollbackMeasurement
				})
				if !errors.Is(err, errRollbackMeasurement) {
					t.Fatal(err)
				}
				var plan []map[string]any
				if err := json.Unmarshal([]byte(encoded), &plan); err != nil {
					t.Fatal(err)
				}
				root := plan[0]["Plan"].(map[string]any)
				s := sample{ms: plan[0]["Execution Time"].(float64)}
				s.walRecords, _ = root["WAL Records"].(float64)
				s.walBytes, _ = root["WAL Bytes"].(float64)
				samples[phase.name] = append(samples[phase.name], s)
			}
		}
		medians := map[string]sample{}
		for _, phase := range phases {
			list := samples[phase.name]
			sort.Slice(list, func(i, j int) bool { return list[i].ms < list[j].ms })
			medians[phase.name] = list[len(list)/2]
			t.Logf("%s %s owners=%d inventory=50000 median_ms=%.3f WAL_records=%.0f WAL_bytes=%.0f", fixture, phase.name, len(entries), medians[phase.name].ms, medians[phase.name].walRecords, medians[phase.name].walBytes)
		}
		return medians
	}
	dead := measure("dead_route", saturated)
	measure("failing_route", failing)
	// Interleaved trials can leave a few records of page pruning for the
	// legacy statement's rolled-back versions; row writes would be ~1,000.
	if dead["legacy"].walRecords < 100 || dead["saturating"].walRecords*100 > dead["legacy"].walRecords {
		t.Fatal("saturation did not remove dead-route WAL", dead)
	}
}
