package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestMetadataWritesOnlyChangedValuesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := ensureProxyQueryIndexSchema(db); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type,hash)
SELECT n,'192.0.2.1','192.0.2.1',3128,'N/A','N/A',decode(md5(n::text),'hex') FROM generate_series(1,10000) n`,
		`CREATE TABLE observed_metadata_updates (proxy_id bigint)`,
		`CREATE FUNCTION observe_metadata_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
INSERT INTO observed_metadata_updates VALUES(NEW.id); RETURN NEW; END $$`,
		`CREATE TRIGGER observe_metadata_update AFTER UPDATE ON proxies FOR EACH ROW EXECUTE FUNCTION observe_metadata_update()`,
		`ANALYZE proxies`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	batch := make([]domain.Proxy, enrichmentUpdateBatchSize)
	for i := range batch {
		batch[i] = domain.Proxy{ID: uint64(1 + 9999*i/(len(batch)-1)), Country: "N/A", EstimatedType: "N/A"}
	}
	batch[1].Country = "DE"
	batch[2].EstimatedType = "Residential"
	locked := db.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	t.Cleanup(func() { _ = locked.Rollback().Error })
	if err := locked.Exec("SELECT id FROM proxies WHERE id=1 FOR UPDATE").Error; err != nil {
		t.Fatal(err)
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	DB = db.WithContext(ctx).Session(&gorm.Session{Logger: queries})
	if err := updateProxyMetadataBatch(batch); err != nil {
		t.Fatal("unchanged metadata waited for a locked route", err)
	}
	_ = locked.Rollback().Error
	if len(queries.queries) != 1 {
		t.Fatal("metadata guard added database statements", queries.queries)
	}
	guardedSQL := queries.queries[0]
	DB = db
	if err := updateProxyMetadataBatch(batch); err != nil {
		t.Fatal(err)
	}
	var observed []uint64
	if err := db.Table("observed_metadata_updates").Order("proxy_id").Pluck("proxy_id", &observed).Error; err != nil {
		t.Fatal(err)
	}
	if len(observed) != 2 || observed[0] != batch[1].ID || observed[1] != batch[2].ID {
		t.Fatal("metadata writes changed equal rows or missed real changes", observed)
	}
	var unexpected int64
	if err := db.Raw(`SELECT count(*) FROM proxies WHERE host<>'192.0.2.1' OR ip_address<>'192.0.2.1'::inet
OR port<>3128 OR hash<>decode(md5(id::text),'hex')`).Scan(&unexpected).Error; err != nil || unexpected != 0 {
		t.Fatal("metadata write changed route identity", unexpected, err)
	}
	if err := db.Exec("DROP TRIGGER observe_metadata_update ON proxies").Error; err != nil {
		t.Fatal(err)
	}
	guard := "\n  AND (p.country, p.estimated_type) IS DISTINCT FROM (tmp.country, tmp.estimated_type)"
	legacySQL := strings.TrimSuffix(guardedSQL, guard)
	if legacySQL == guardedSQL {
		t.Fatal("metadata guard was not captured", guardedSQL)
	}
	var originalWAL, guardedWAL float64
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
			root := plan[0]["Plan"].(map[string]any)
			wal, _ := root["WAL Records"].(float64)
			if phase.name == "original" {
				originalWAL += wal
			} else {
				guardedWAL += wal
			}
			t.Logf("%s metadata_rows=512 inventory=10000 trial=%d execution_ms=%v WAL_records=%v WAL_bytes=%v", phase.name, trial, plan[0]["Execution Time"], root["WAL Records"], root["WAL Bytes"])
			if directory := os.Getenv("MAGPIE_TEST_WRITE_GUARD_EVIDENCE_DIR"); directory != "" {
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, fmt.Sprintf("metadata-%s-%d-plan.json", phase.name, trial)), []byte(encoded), 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if originalWAL == 0 || guardedWAL >= originalWAL {
		t.Fatal("guard did not reduce measured WAL for repeated metadata", originalWAL, guardedWAL)
	}
}

func TestCheckUsageDoesNotWaitForSubscriptionLockPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace, _, _, _ := seedCheckerEvidenceTest(t, db)
	subscription := domain.WorkspaceSubscription{WorkspaceID: workspace.ID, OverageMode: domain.WorkspaceOverageUnlimited}
	if err := db.Create(&subscription).Error; err != nil {
		t.Fatal(err)
	}
	if err := UpdateWorkspaceUsageActiveRoutes(db, workspace.ID, 1000000); err != nil {
		t.Fatal(err)
	}
	locked := db.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	t.Cleanup(func() { _ = locked.Rollback().Error })
	if err := locked.Exec("SELECT workspace_id FROM workspace_subscriptions WHERE workspace_id=? FOR UPDATE", workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := db.WithContext(ctx).Session(&gorm.Session{Logger: queries}).Transaction(func(tx *gorm.DB) error {
		return recordWorkspaceCheckUsage(tx, []domain.ProxyStatistic{{WorkspaceIDs: []uint{workspace.ID, workspace.ID}, Attempt: 2, CreatedAt: time.Now().UTC()}})
	}); err != nil {
		t.Fatal("check usage waited for a subscription lock", err)
	}
	for _, sql := range queries.queries {
		if strings.Contains(sql, "user_proxies") || strings.Contains(sql, "workspace_subscriptions") {
			t.Fatal("check usage touched inventory or subscription locks", sql)
		}
	}
	var usage domain.WorkspaceUsagePeriod
	if err := db.First(&usage, "workspace_id=?", workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	if usage.ActiveRoutes != 1000000 || usage.PeakActiveRoutes != 1000000 || usage.CheckAttempts != 3 {
		t.Fatal("check usage lost capacity or counted a duplicated owner twice", usage)
	}
	t.Logf("check_delta_write_with_subscription_locked=%s active_routes=1000000 counted_attempts=3", time.Since(start))
}
