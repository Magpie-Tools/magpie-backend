package database

import (
	"fmt"
	"gorm.io/gorm"
	"magpie/internal/domain"
	"strings"
)

func prepareCheckerSettingsSchema(db *gorm.DB) error {
	if db.Dialector.Name() != "postgres" || !db.Migrator().HasTable(&domain.ProxyLatestStatistic{}) {
		return nil
	}
	for _, column := range []string{"workspace_id bigint NOT NULL DEFAULT 0", "config_key varchar(64) NOT NULL DEFAULT ''", "transport_protocol varchar(8) NOT NULL DEFAULT ''"} {
		if err := db.Exec("ALTER TABLE proxy_latest_statistics ADD COLUMN IF NOT EXISTS " + column).Error; err != nil {
			return err
		}
	}
	var primary struct {
		Name  string
		Count int
	}
	if err := db.Raw(`SELECT conname AS name, cardinality(conkey) AS count FROM pg_constraint WHERE conrelid = 'proxy_latest_statistics'::regclass AND contype = 'p'`).Scan(&primary).Error; err != nil {
		return err
	}
	if primary.Count == 4 {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if primary.Name != "" {
			if err := tx.Exec(`ALTER TABLE proxy_latest_statistics DROP CONSTRAINT "` + strings.ReplaceAll(primary.Name, `"`, `""`) + `"`).Error; err != nil {
				return err
			}
		}
		return tx.Exec("ALTER TABLE proxy_latest_statistics ADD PRIMARY KEY (workspace_id, config_key, proxy_id, protocol_id)").Error
	})
}

func ensureCheckerSettingsSchema(db *gorm.DB) error {
	if !db.Migrator().HasTable(&domain.Workspace{}) {
		return nil
	}
	if err := db.AutoMigrate(&domain.ProxyCheckerPlan{}, &domain.CheckerProxyChange{}); err != nil {
		return err
	}
	if db.Dialector.Name() == "postgres" && db.Migrator().HasTable(&domain.ManagedProxy{}) {
		if err := db.Exec(`DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'proxy_checker_plans'::regclass AND conname = 'fk_proxy_checker_plans_managed_proxy') THEN
ALTER TABLE proxy_checker_plans ADD CONSTRAINT fk_proxy_checker_plans_managed_proxy FOREIGN KEY (workspace_id, proxy_id) REFERENCES user_proxies (workspace_id, proxy_id) ON UPDATE CASCADE ON DELETE CASCADE;
END IF; END $$;`).Error; err != nil {
			return err
		}
	}
	if db.Dialector.Name() == "postgres" && db.Migrator().HasTable(&domain.ManagedProxy{}) {
		if err := db.Exec(`ALTER TABLE checker_proxy_changes DROP CONSTRAINT IF EXISTS fk_checker_proxy_changes_managed_proxy`).Error; err != nil {
			return err
		}
		if err := db.Exec(`DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'checker_proxy_changes'::regclass AND conname = 'fk_checker_proxy_changes_workspace') THEN
ALTER TABLE checker_proxy_changes ADD CONSTRAINT fk_checker_proxy_changes_workspace FOREIGN KEY (workspace_id) REFERENCES workspaces (id) ON UPDATE CASCADE ON DELETE CASCADE;
END IF; END $$;`).Error; err != nil {
			return err
		}
	}
	if db.Dialector.Name() != "postgres" || !db.Migrator().HasTable(&domain.ProxyStatistic{}) {
		return nil
	}
	for _, column := range []string{"check_evidence jsonb", "transport_protocol varchar(8) NOT NULL DEFAULT ''", "check_timeout integer NOT NULL DEFAULT 0", "check_retries smallint NOT NULL DEFAULT 0"} {
		if err := db.Exec("ALTER TABLE proxy_statistics ADD COLUMN IF NOT EXISTS " + column).Error; err != nil {
			return fmt.Errorf("checker history attribution: %w", err)
		}
	}
	return nil
}
