package schema

import (
	"fmt"

	"magpie/internal/platform/pg"

	"gorm.io/gorm"
)

func EnsureReadModelSchema(db *gorm.DB) error {
	if db == nil || !pg.IsPostgresDialect(db) {
		return nil
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS user_proxy_filter_indexes (
			workspace_id bigint NOT NULL,
			proxy_id bigint NOT NULL,
			PRIMARY KEY (workspace_id, proxy_id)
		)`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS host varchar(253)`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS state varchar(16) NOT NULL DEFAULT 'active'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS pause_reason varchar(24) NOT NULL DEFAULT ''`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS ip_address inet`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS port integer NOT NULL DEFAULT 0`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS country varchar(56) NOT NULL DEFAULT 'N/A'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS country_key varchar(56) NOT NULL DEFAULT 'n/a'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS estimated_type varchar(20) NOT NULL DEFAULT 'N/A'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS type_key varchar(20) NOT NULL DEFAULT 'n/a'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS anonymity_level varchar(50) NOT NULL DEFAULT 'N/A'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS anonymity_key varchar(50) NOT NULL DEFAULT 'n/a'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS alive boolean NOT NULL DEFAULT false`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS latest_check timestamptz NOT NULL DEFAULT '0001-01-01 00:00:00+00'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS response_time integer NOT NULL DEFAULT 0`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS attempt integer NOT NULL DEFAULT 0`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS health_overall real`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS health_http real`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS health_https real`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS health_socks4 real`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS health_socks5 real`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS alive_http boolean NOT NULL DEFAULT false`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS alive_https boolean NOT NULL DEFAULT false`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS alive_socks4 boolean NOT NULL DEFAULT false`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS alive_socks5 boolean NOT NULL DEFAULT false`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS reputation_label varchar(16) NOT NULL DEFAULT 'unknown'`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS reputation_score real`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP`,
		`ALTER TABLE user_proxy_filter_indexes ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_recent_active ON user_proxy_filter_indexes (workspace_id, alive DESC, latest_check DESC, proxy_id ASC) WHERE state = 'active'`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_user_alive_latest ON user_proxy_filter_indexes (workspace_id, alive, latest_check DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_user_country ON user_proxy_filter_indexes (workspace_id, country_key)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_user_type ON user_proxy_filter_indexes (workspace_id, type_key)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_user_reputation ON user_proxy_filter_indexes (workspace_id, reputation_label, reputation_score DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_proxy_id ON user_proxy_filter_indexes (proxy_id)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_host ON user_proxy_filter_indexes (host)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_ip_address ON user_proxy_filter_indexes (ip_address)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_ip_address_gist ON user_proxy_filter_indexes USING gist (ip_address inet_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_port ON user_proxy_filter_indexes (port)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_anonymity_key ON user_proxy_filter_indexes (anonymity_key)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_alive_http ON user_proxy_filter_indexes (alive_http)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_alive_https ON user_proxy_filter_indexes (alive_https)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_alive_socks4 ON user_proxy_filter_indexes (alive_socks4)`,
		`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_alive_socks5 ON user_proxy_filter_indexes (alive_socks5)`,
		`CREATE TABLE IF NOT EXISTS user_scrape_source_stats (
			workspace_id bigint NOT NULL,
			scrape_site_id bigint NOT NULL,
			PRIMARY KEY (workspace_id, scrape_site_id)
		)`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS url text NOT NULL DEFAULT ''`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS protocol_key varchar(16) NOT NULL DEFAULT ''`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS proxy_count bigint NOT NULL DEFAULT 0`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS alive_count bigint NOT NULL DEFAULT 0`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS dead_count bigint NOT NULL DEFAULT 0`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS unknown_count bigint NOT NULL DEFAULT 0`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS added_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP`,
		`ALTER TABLE user_scrape_source_stats ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP`,
		`CREATE INDEX IF NOT EXISTS idx_user_scrape_source_stats_user_added ON user_scrape_source_stats (workspace_id, added_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_user_scrape_source_stats_user_protocol ON user_scrape_source_stats (workspace_id, protocol_key)`,
		`CREATE INDEX IF NOT EXISTS idx_user_scrape_source_stats_proxy_count ON user_scrape_source_stats (proxy_count)`,
		`CREATE INDEX IF NOT EXISTS idx_user_scrape_source_stats_alive_count ON user_scrape_source_stats (alive_count)`,
	}

	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("read model schema: %w", err)
		}
	}

	return removeDuplicateProxyFilterIndexes(db)
}

// Earlier GORM tags and the explicit schema used different names for these
// indexes. Remove only those known copies, and only while a valid canonical
// index has the same access method, keys, ordering, predicates and constraints.
func removeDuplicateProxyFilterIndexes(db *gorm.DB) error {
	var indexes []struct{ SchemaName, IndexName string }
	if err := db.Raw(`
WITH candidates(legacy_name, canonical_name) AS (VALUES
	('idx_workspace_proxy_filter_alive_latest', 'idx_user_proxy_filter_user_alive_latest'),
	('idx_workspace_proxy_filter_country', 'idx_user_proxy_filter_user_country'),
	('idx_workspace_proxy_filter_type', 'idx_user_proxy_filter_user_type'),
	('idx_workspace_proxy_filter_reputation', 'idx_user_proxy_filter_user_reputation'),
	('idx_user_proxy_filter_indexes_proxy_id', 'idx_user_proxy_filter_proxy_id'),
	('idx_user_proxy_filter_indexes_host', 'idx_user_proxy_filter_host'),
	('idx_user_proxy_filter_indexes_ip_address', 'idx_user_proxy_filter_ip_address'),
	('idx_user_proxy_filter_indexes_port', 'idx_user_proxy_filter_port'),
	('idx_user_proxy_filter_indexes_anonymity_key', 'idx_user_proxy_filter_anonymity_key'),
	('idx_user_proxy_filter_indexes_alive_http', 'idx_user_proxy_filter_alive_http'),
	('idx_user_proxy_filter_indexes_alive_http_s', 'idx_user_proxy_filter_alive_https'),
	('idx_user_proxy_filter_indexes_alive_socks4', 'idx_user_proxy_filter_alive_socks4'),
	('idx_user_proxy_filter_indexes_alive_socks5', 'idx_user_proxy_filter_alive_socks5')
)
SELECT ns.nspname AS schema_name, old_class.relname AS index_name
FROM candidates c
JOIN pg_index old_idx ON old_idx.indrelid = 'user_proxy_filter_indexes'::regclass
JOIN pg_class old_class ON old_class.oid = old_idx.indexrelid AND old_class.relname = c.legacy_name
JOIN pg_namespace ns ON ns.oid = old_class.relnamespace
JOIN pg_index keep_idx ON keep_idx.indrelid = old_idx.indrelid
JOIN pg_class keep_class ON keep_class.oid = keep_idx.indexrelid AND keep_class.relname = c.canonical_name
WHERE old_class.relkind = 'i' AND keep_class.relkind = 'i'
	AND old_class.relam = keep_class.relam
	AND old_class.reltablespace = keep_class.reltablespace
	AND old_class.reloptions IS NOT DISTINCT FROM keep_class.reloptions
	AND old_idx.indisvalid AND old_idx.indisready AND old_idx.indislive
	AND keep_idx.indisvalid AND keep_idx.indisready AND keep_idx.indislive
	AND NOT old_idx.indisprimary AND NOT old_idx.indisreplident AND NOT old_idx.indisclustered
	AND old_idx.indisunique = keep_idx.indisunique
	-- PostgreSQL 14 has no indnullsnotdistinct column; both JSON keys are absent.
	AND to_jsonb(old_idx)->'indnullsnotdistinct' IS NOT DISTINCT FROM to_jsonb(keep_idx)->'indnullsnotdistinct'
	AND old_idx.indisexclusion = keep_idx.indisexclusion
	AND old_idx.indimmediate = keep_idx.indimmediate
	AND old_idx.indnatts = keep_idx.indnatts
	AND old_idx.indnkeyatts = keep_idx.indnkeyatts
	AND old_idx.indkey = keep_idx.indkey
	AND old_idx.indcollation = keep_idx.indcollation
	AND old_idx.indclass = keep_idx.indclass
	AND old_idx.indoption = keep_idx.indoption
	AND old_idx.indexprs::text IS NOT DISTINCT FROM keep_idx.indexprs::text
	AND old_idx.indpred::text IS NOT DISTINCT FROM keep_idx.indpred::text
	AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = old_idx.indexrelid)
ORDER BY old_class.relname`).Scan(&indexes).Error; err != nil {
		return fmt.Errorf("read model schema: compare duplicate indexes: %w", err)
	}
	for _, index := range indexes {
		if err := db.Exec("DROP INDEX IF EXISTS " + pg.QuoteIdentifier(index.SchemaName) + "." + pg.QuoteIdentifier(index.IndexName)).Error; err != nil {
			return fmt.Errorf("read model schema: drop duplicate index %s: %w", index.IndexName, err)
		}
	}
	return nil
}
