package database

import (
	"reflect"
	"strings"
	"testing"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

// These are the physical copies present in the release rehearsal, before the
// model tags and explicit schema agreed on index names.
var oldProxyFilterIndexNames = map[string]string{
	"idx_workspace_proxy_filter_alive_latest":     "idx_user_proxy_filter_user_alive_latest",
	"idx_workspace_proxy_filter_country":          "idx_user_proxy_filter_user_country",
	"idx_workspace_proxy_filter_type":             "idx_user_proxy_filter_user_type",
	"idx_workspace_proxy_filter_reputation":       "idx_user_proxy_filter_user_reputation",
	"idx_user_proxy_filter_indexes_proxy_id":      "idx_user_proxy_filter_proxy_id",
	"idx_user_proxy_filter_indexes_host":          "idx_user_proxy_filter_host",
	"idx_user_proxy_filter_indexes_ip_address":    "idx_user_proxy_filter_ip_address",
	"idx_user_proxy_filter_indexes_port":          "idx_user_proxy_filter_port",
	"idx_user_proxy_filter_indexes_anonymity_key": "idx_user_proxy_filter_anonymity_key",
	"idx_user_proxy_filter_indexes_alive_http":    "idx_user_proxy_filter_alive_http",
	"idx_user_proxy_filter_indexes_alive_http_s":  "idx_user_proxy_filter_alive_https",
	"idx_user_proxy_filter_indexes_alive_socks4":  "idx_user_proxy_filter_alive_socks4",
	"idx_user_proxy_filter_indexes_alive_socks5":  "idx_user_proxy_filter_alive_socks5",
}

func proxyFilterIndexDefinitions(t testing.TB, db *gorm.DB) map[string]string {
	t.Helper()
	var rows []struct{ Name, Definition string }
	if err := db.Raw(`SELECT c.relname AS name, pg_get_indexdef(c.oid) AS definition
FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
WHERE i.indrelid='user_proxy_filter_indexes'::regclass ORDER BY c.relname`).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string, len(rows))
	for _, row := range rows {
		result[row.Name] = row.Definition
	}
	return result
}

func createOldProxyFilterIndexes(t testing.TB, db *gorm.DB) {
	t.Helper()
	definitions := proxyFilterIndexDefinitions(t, db)
	for old, canonical := range oldProxyFilterIndexNames {
		definition := definitions[canonical]
		if definition == "" {
			t.Fatal("canonical index missing", canonical)
		}
		prefix := "CREATE INDEX " + canonical + " ON "
		if !strings.HasPrefix(definition, prefix) {
			t.Fatal("unexpected canonical definition", definition)
		}
		if err := db.Exec("CREATE INDEX " + quoteIdentifier(old) + " ON " + strings.TrimPrefix(definition, prefix)).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func TestProxyFilterIndexStartupDeduplicatesAndStaysIdempotentPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := ensureProxyListPerformanceSchema(db); err != nil {
		t.Fatal(err)
	}
	canonical := proxyFilterIndexDefinitions(t, db)
	createOldProxyFilterIndexes(t, db)
	if err := db.Exec(`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,port,alive,latest_check)
SELECT w,n,'192.0.2.1',8080,n%2=0,'2026-10-01'::timestamptz+n*interval '1 second'
FROM generate_series(1,8) w CROSS JOIN generate_series(1,24) n`).Error; err != nil {
		t.Fatal(err)
	}
	var before string
	if err := db.Raw(`SELECT md5(string_agg(row_to_json(t)::text,',' ORDER BY workspace_id,proxy_id))
FROM user_proxy_filter_indexes t`).Scan(&before).Error; err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		// SetupDB performs AutoMigrate before the explicit schema initializer.
		if err := db.AutoMigrate(&domain.WorkspaceProxyFilterIndex{}); err != nil {
			t.Fatal(err)
		}
		if err := ensureReadModelSchema(db); err != nil {
			t.Fatal(err)
		}
		if got := proxyFilterIndexDefinitions(t, db); !reflect.DeepEqual(got, canonical) {
			t.Fatalf("startup %d changed canonical indexes or recreated old copies:\ngot=%v\nwant=%v", attempt, got, canonical)
		}
	}
	var after string
	if err := db.Raw(`SELECT md5(string_agg(row_to_json(t)::text,',' ORDER BY workspace_id,proxy_id))
FROM user_proxy_filter_indexes t`).Scan(&after).Error; err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("index migration changed projection data", before, after)
	}
}

func TestProxyFilterIndexCleanupPreservesDifferentAndCustomIndexesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE INDEX custom_host_copy ON user_proxy_filter_indexes (host)`).Error; err != nil {
		t.Fatal(err)
	}
	cases := []struct{ name, oldName, definition string }{
		{"sort", "idx_workspace_proxy_filter_alive_latest", "(workspace_id, alive DESC, latest_check DESC)"},
		{"nulls", "idx_workspace_proxy_filter_alive_latest", "(workspace_id, alive, latest_check DESC NULLS LAST)"},
		{"predicate", "idx_user_proxy_filter_indexes_host", "(host) WHERE state='active'"},
		{"opclass", "idx_user_proxy_filter_indexes_host", "(host varchar_pattern_ops)"},
		{"collation", "idx_user_proxy_filter_indexes_host", "(host COLLATE \"C\")"},
		{"access_method", "idx_user_proxy_filter_indexes_host", "USING hash (host)"},
		{"include", "idx_user_proxy_filter_indexes_host", "(host) INCLUDE (port)"},
		{"expression", "idx_user_proxy_filter_indexes_host", "(lower(host))"},
		{"storage", "idx_user_proxy_filter_indexes_host", "(host) WITH (fillfactor=50)"},
		{"unique", "idx_user_proxy_filter_indexes_host", "UNIQUE (host)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			statement := "CREATE INDEX " + quoteIdentifier(tc.oldName) + " ON user_proxy_filter_indexes " + tc.definition
			if strings.HasPrefix(tc.definition, "UNIQUE ") {
				statement = "CREATE UNIQUE INDEX " + quoteIdentifier(tc.oldName) + " ON user_proxy_filter_indexes " + strings.TrimPrefix(tc.definition, "UNIQUE ")
			}
			if err := db.Exec(statement).Error; err != nil {
				t.Fatal(err)
			}
			before := proxyFilterIndexDefinitions(t, db)
			if err := ensureReadModelSchema(db); err != nil {
				t.Fatal(err)
			}
			if got := proxyFilterIndexDefinitions(t, db); !reflect.DeepEqual(got, before) {
				t.Fatal("cleanup removed an index with different semantics or an unknown custom name", got, before)
			}
			if err := db.Exec("DROP INDEX " + quoteIdentifier(tc.oldName)).Error; err != nil {
				t.Fatal(err)
			}
		})
	}
	// A known old name can also designate the table's chosen cluster order.
	if err := db.Exec(`CREATE INDEX idx_user_proxy_filter_indexes_host ON user_proxy_filter_indexes (host)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`ALTER TABLE user_proxy_filter_indexes CLUSTER ON idx_user_proxy_filter_indexes_host`).Error; err != nil {
		t.Fatal(err)
	}
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	if _, ok := proxyFilterIndexDefinitions(t, db)["idx_user_proxy_filter_indexes_host"]; !ok {
		t.Fatal("cleanup removed the user's chosen cluster index")
	}
	t.Run("unique_null_semantics", func(t *testing.T) {
		var version int
		if err := db.Raw("SHOW server_version_num").Scan(&version).Error; err != nil {
			t.Fatal(err)
		}
		if version < 150000 {
			t.Skip("NULLS NOT DISTINCT indexes require PostgreSQL 15")
		}
		for _, statement := range []string{
			`DROP INDEX idx_user_proxy_filter_port`,
			`CREATE UNIQUE INDEX idx_user_proxy_filter_port ON user_proxy_filter_indexes (port) NULLS NOT DISTINCT`,
			`CREATE UNIQUE INDEX idx_user_proxy_filter_indexes_port ON user_proxy_filter_indexes (port)`,
		} {
			if err := db.Exec(statement).Error; err != nil {
				t.Fatal(err)
			}
		}
		before := proxyFilterIndexDefinitions(t, db)
		if err := ensureReadModelSchema(db); err != nil {
			t.Fatal(err)
		}
		if got := proxyFilterIndexDefinitions(t, db); !reflect.DeepEqual(got, before) {
			t.Fatal("cleanup removed a unique index with different NULL semantics", got, before)
		}
	})
}
