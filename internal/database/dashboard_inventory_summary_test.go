package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"magpie/internal/domain"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestDashboardInventorySummarySemantics(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var db *gorm.DB
			if dialect == "postgres" {
				db = setupProxyIngestionPostgresTest(t)
			} else {
				var err error
				db, err = gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "inventory.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
				if err != nil {
					t.Fatal(err)
				}
				sqlDB, _ := db.DB()
				t.Cleanup(func() { _ = sqlDB.Close() })
				if err := db.Exec(`CREATE TABLE user_proxy_filter_indexes (
					workspace_id bigint, proxy_id bigint, state text, country text, country_key text,
					reputation_label text, reputation_score real, host text, port integer,
					PRIMARY KEY (workspace_id,proxy_id))`).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Exec(`INSERT INTO user_proxy_filter_indexes
				(workspace_id,proxy_id,state,country,country_key,reputation_label,reputation_score,host,port) VALUES
				(2,10,'active','DE','de','good',90,'winner.example',8080),
				(2,11,'active','DE','de','good',90,'tie.example',8081),
				(2,12,'active','FR','fr','neutral',NULL,'null.example',8082),
				(2,13,'active','N/A','n/a','custom',-5,'negative.example',8083),
				(2,14,'active','Unknown','unknown','unknown',NULL,'unknown.example',8084),
				(2,15,'active','Zz','zz','poor',0,'zero.example',8085),
				(2,16,'active','FR','fr','neutral',NULL,'second.example',8086),
				(2,1,'paused','US','us','good',999,'paused.example',1),
				(2,2,'archived','GB','gb','good',999,'archived.example',2),
				(3,10,'active','JP','jp','good',999,'other-owner.example',3),
				(3,3,'active','JP','jp','good',999,'unrelated.example',4)`).Error; err != nil {
				t.Fatal(err)
			}
			queries := &dashboardStartupQueryLog{Interface: logger.Default.LogMode(logger.Silent)}
			got, err := loadDashboardInventorySummary(db.Session(&gorm.Session{Logger: queries}), 2)
			if err != nil {
				t.Fatal(err)
			}
			wantCountries := []dashboardCountryCount{{"DE", 2}, {"FR", 2}, {"Unknown", 2}, {"Zz", 1}}
			if !reflect.DeepEqual(got.Countries, wantCountries) {
				t.Fatalf("country counts/order=%v want=%v", got.Countries, wantCountries)
			}
			labels := make(map[string]uint)
			for _, row := range got.Reputations {
				labels[row.Label] = row.Count
			}
			if !reflect.DeepEqual(labels, map[string]uint{"good": 2, "neutral": 2, "custom": 1, "unknown": 1, "poor": 1}) {
				t.Fatal("reputation counts changed", labels)
			}
			if !got.TopProxyFound || got.TopProxy != (dashboardTopReputationProxy{ProxyID: 10, IP: "winner.example", Port: 8080, Score: 90, Label: "good"}) {
				t.Fatal("top score, metadata, tie order or tenant/lifecycle scope changed", got.TopProxy)
			}
			if len(queries.queries) != 1 {
				t.Fatal("inventory summaries require one round trip", len(queries.queries))
			}
			if err := db.Exec(`DELETE FROM user_proxy_filter_indexes WHERE workspace_id=2 AND reputation_score IS NOT NULL AND state='active'`).Error; err != nil {
				t.Fatal(err)
			}
			got, err = loadDashboardInventorySummary(db, 2)
			if err != nil || got.TopProxyFound || len(got.Countries) != 2 {
				t.Fatal("null scores must count without producing a top proxy", got, err)
			}
			got, err = loadDashboardInventorySummary(db, 99)
			if err != nil || !reflect.DeepEqual(got, dashboardInventorySummary{}) {
				t.Fatal("empty workspace produced inventory", got, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := loadDashboardInventorySummary(db.WithContext(ctx), 2); err == nil {
				t.Fatal("canceled summary succeeded")
			}
			if err := db.Exec(`DROP TABLE user_proxy_filter_indexes`).Error; err != nil {
				t.Fatal(err)
			}
			if got, err := loadDashboardInventorySummary(db, 2); err == nil || !reflect.DeepEqual(got, dashboardInventorySummary{}) {
				t.Fatal("failed query returned a partial summary", got, err)
			}
		})
	}
}

func TestDashboardInventorySummarySharedScanPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspaces := make([]domain.Workspace, 8)
	for i := range workspaces {
		workspaces[i] = domain.Workspace{Name: fmt.Sprintf("Inventory summary owner %d", i)}
	}
	if err := db.Create(&workspaces).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxy_filter_indexes
		(workspace_id,proxy_id,state,country,country_key,reputation_label,reputation_score,host,port)
		SELECT w.id,n,'active',CASE WHEN n%2=0 THEN 'DE' ELSE 'FR' END,
		CASE WHEN n%2=0 THEN 'de' ELSE 'fr' END,'good',
		CASE WHEN n%13=0 THEN 100 ELSE NULL END,'dense.example',8080
		FROM workspaces w CROSS JOIN generate_series(1,10000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`ANALYZE user_proxy_filter_indexes`).Error; err != nil {
		t.Fatal(err)
	}
	queries := &dashboardStartupQueryLog{Interface: logger.Default.LogMode(logger.Silent)}
	got, err := loadDashboardInventorySummary(db.Session(&gorm.Session{Logger: queries}), workspaces[0].ID)
	if err != nil || !got.TopProxyFound || got.TopProxy.ProxyID != 13 ||
		!reflect.DeepEqual(got.Countries, []dashboardCountryCount{{"DE", 5000}, {"FR", 5000}}) ||
		!reflect.DeepEqual(got.Reputations, []dashboardReputationCount{{"good", 10000}}) {
		t.Fatal("shared inventory result changed", got, err)
	}
	if len(queries.queries) != 1 {
		t.Fatal("shared summaries used multiple round trips", len(queries.queries))
	}
	query := queries.queries[0]
	var encoded string
	if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query).Row().Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var plans []struct{ Plan map[string]any }
	if err := json.Unmarshal([]byte(encoded), &plans); err != nil {
		t.Fatal(err)
	}
	var scans int
	visitProxyReadPlan(plans[0].Plan, func(node map[string]any) {
		if node["Relation Name"] == "user_proxy_filter_indexes" {
			scans++
			if loops, _ := node["Actual Loops"].(float64); loops > 4 {
				t.Fatal("summary repeated base inventory probes", node)
			}
		}
	})
	if scans != 1 {
		t.Fatal("summary scanned its base projection more than once", scans)
	}
	if directory := os.Getenv("MAGPIE_TEST_DASHBOARD_INVENTORY_EVIDENCE_DIR"); directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string][]byte{"production.sql": []byte(query), "shared-scan-plan.json": []byte(encoded)} {
			if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
