package schema

import "magpie/internal/domain"

import (
	"fmt"
	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

import "magpie/internal/platform/pg"
import "magpie/internal/support"

func DefaultMigrations() []any {
	return []any{
		domain.User{},
		domain.NotificationRead{},
		domain.Workspace{},
		domain.WorkspaceMembership{},
		domain.WorkspaceInvitation{},
		domain.WorkspaceMemberPreference{},
		domain.WorkspaceSubscription{},
		domain.WorkspaceUsagePeriod{},
		domain.WorkspaceUsageFlush{},
		domain.PasswordResetToken{},
		domain.EmailOutbox{},
		domain.AlertRule{},
		domain.AlertDestination{},
		domain.AlertIncident{},
		domain.AlertDelivery{},
		// Register the full managed-proxy join schema before Proxy/Workspace
		// relationships can ask GORM to create a two-column join table.
		domain.ManagedProxy{},
		domain.Proxy{},
		domain.BlacklistedIP{},
		domain.BlacklistedRange{},
		domain.AbuseIPDBCheck{},
		domain.IPGeolocationLookup{},
		domain.IPGeolocationState{},
		domain.ProxyReputation{},
		domain.ProxyReputationRefresh{},
		domain.ProxyTag{},
		domain.ProxyTagAssignment{},
		domain.ProxyCheckerPlan{},
		domain.CheckerProxyChange{},
		domain.UserProxyFilterIndex{},
		domain.UserScrapeSourceStat{},
		domain.ProxyDailyCheck{},
		domain.ProxyCheckTotal{},
		domain.ProxyDailyCheckProxyBackfill{},
		domain.RotatingProxy{},
		domain.ProxyHistory{},
		domain.ProxySnapshot{},
		domain.ProxyStatistic{},
		domain.ProxyStatisticEvent{},
		domain.ProxyStatisticStream{},
		domain.ProxyLatestStatistic{},
		domain.ProxyOverallStatus{},
		domain.AnonymityLevel{},
		domain.Judge{},
		domain.WorkspaceJudge{},
		domain.ScrapeSite{},
		domain.UserScrapeSite{},
		domain.ScrapeSourceTag{},
		domain.ProxyScrapeSite{},
		domain.Protocol{},
	}
}

func SeedDefaults(db *gorm.DB) error {
	if err := ensureAnonymityLevels(db); err != nil {
		return err
	}
	if err := ensureProtocols(db); err != nil {
		return err
	}
	return nil
}

func ensureAnonymityLevels(db *gorm.DB) error {
	if !db.Migrator().HasTable(&domain.AnonymityLevel{}) {
		return nil
	}

	var count int64
	if err := db.Model(&domain.AnonymityLevel{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	levels := []domain.AnonymityLevel{
		{Name: "elite"},
		{Name: "anonymous"},
		{Name: "transparent"},
	}

	return db.Create(&levels).Error
}

func ensureProtocols(db *gorm.DB) error {
	if !db.Migrator().HasTable(&domain.Protocol{}) {
		return nil
	}

	var count int64
	if err := db.Model(&domain.Protocol{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	protocols := []domain.Protocol{
		{Name: "http", ID: 1},
		{Name: "https", ID: 2},
		{Name: "socks4", ID: 3},
		{Name: "socks5", ID: 4},
	}

	return db.Create(&protocols).Error
}

type schemaEnsureStep struct {
	name string
	run  func(*gorm.DB) error
}

func EnsurePostMigrateSchemas(db *gorm.DB) error {
	steps := []schemaEnsureStep{
		{name: "user auth schema", run: ensureUserAuthSchema},
		{name: "workspace schema", run: ensureWorkspaceSchema},
		{name: "checker settings schema", run: EnsureCheckerSettingsSchema},
		{name: "proxy access storage schema", run: EnsureProxyAccessStorageSchema},
		{name: "proxy reputation schema", run: EnsureProxyReputationSchema},
		{name: "proxy statistics retention schema", run: EnsureProxyStatisticsRetentionSchema},
		{name: "proxy timeline retention schema", run: ensureProxyTimelineRetentionSchema},
		{name: "proxy query index schema", run: EnsureProxyQueryIndexSchema},
		{name: "rotating proxy schema", run: ensureRotatingProxySchema},
		{name: "blacklist schema", run: ensureBlacklistSchema},
	}

	return runSchemaEnsureSteps(db, steps)
}

func ensureUserAuthSchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}
	if !db.Migrator().HasTable(&domain.User{}) {
		return nil
	}

	type duplicateEmailRow struct {
		Normalized string
		Count      int64
	}

	var duplicate duplicateEmailRow
	err := db.Raw(`
		SELECT LOWER(TRIM(email)) AS normalized, COUNT(*) AS count
		FROM users
		GROUP BY LOWER(TRIM(email))
		HAVING COUNT(*) > 1
		LIMIT 1
	`).Scan(&duplicate).Error
	if err != nil {
		return fmt.Errorf("detect duplicate canonical emails: %w", err)
	}
	if strings.TrimSpace(duplicate.Normalized) != "" {
		return fmt.Errorf("duplicate user emails exist after normalization: %s", duplicate.Normalized)
	}

	if err := db.Exec(`UPDATE users SET email = LOWER(TRIM(email)) WHERE email <> LOWER(TRIM(email)) OR email <> TRIM(email)`).Error; err != nil {
		return fmt.Errorf("normalize stored user emails: %w", err)
	}

	stmts := []string{
		`DROP INDEX IF EXISTS idx_users_email`,
		`DROP INDEX IF EXISTS users_email_key`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_lower_unique ON users (LOWER(email))`,
	}
	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("user auth schema: %w", err)
		}
	}

	return nil
}

func runSchemaEnsureSteps(db *gorm.DB, steps []schemaEnsureStep) error {
	for _, step := range steps {
		if err := step.run(db); err != nil {
			return fmt.Errorf("database: ensure %s: %w", step.name, err)
		}
	}
	return nil
}

func ensureBlacklistSchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}
	if !pg.IsPostgresDialect(db) {
		return nil
	}

	stmts := []string{
		`ALTER TABLE IF EXISTS blacklisted_ips ALTER COLUMN ip TYPE inet USING ip::inet`,
		`ALTER TABLE IF EXISTS blacklisted_ips DROP COLUMN IF EXISTS first_seen_at`,
		`ALTER TABLE IF EXISTS blacklisted_ips DROP COLUMN IF EXISTS last_seen_at`,
		`ALTER TABLE IF EXISTS blacklisted_ips SET UNLOGGED`,
		`CREATE INDEX IF NOT EXISTS idx_blacklisted_ips_gist ON blacklisted_ips USING gist (ip inet_ops)`,
		`ALTER TABLE IF EXISTS blacklisted_ranges ADD COLUMN IF NOT EXISTS cidr cidr`,
		`ALTER TABLE IF EXISTS blacklisted_ranges ALTER COLUMN cidr DROP NOT NULL`,
		`DO $$ BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'blacklisted_ranges' AND column_name = 'c_i_d_r'
			) THEN
				UPDATE blacklisted_ranges SET cidr = COALESCE(cidr, c_i_d_r::cidr);
				ALTER TABLE blacklisted_ranges DROP COLUMN IF EXISTS c_i_d_r;
			END IF;
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'blacklisted_ranges' AND column_name = 'c_id_r'
			) THEN
				UPDATE blacklisted_ranges SET cidr = COALESCE(cidr, c_id_r::cidr);
				ALTER TABLE blacklisted_ranges DROP COLUMN IF EXISTS c_id_r;
			END IF;
		END $$;`,
		`ALTER TABLE IF EXISTS blacklisted_ranges DROP COLUMN IF EXISTS start_ip`,
		`ALTER TABLE IF EXISTS blacklisted_ranges DROP COLUMN IF EXISTS end_ip`,
		`ALTER TABLE IF EXISTS blacklisted_ranges DROP COLUMN IF EXISTS first_seen_at`,
		`ALTER TABLE IF EXISTS blacklisted_ranges DROP COLUMN IF EXISTS last_seen_at`,
		`ALTER TABLE IF EXISTS blacklisted_ranges SET UNLOGGED`,
		`DROP INDEX IF EXISTS idx_blacklisted_ranges_cidr`,
		`CREATE INDEX IF NOT EXISTS idx_blacklisted_ranges_cidr_gist ON blacklisted_ranges USING gist (cidr inet_ops)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_blacklisted_ranges_cidr_btree ON blacklisted_ranges (cidr)`,
	}

	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("blacklist schema: %w", err)
		}
	}

	return nil
}

func ensureRotatingProxySchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}
	if !pg.IsPostgresDialect(db) {
		return nil
	}
	if !db.Migrator().HasTable(&domain.RotatingProxy{}) {
		return nil
	}

	currentInstanceID := support.GetInstanceID()
	stmts := []string{
		`ALTER TABLE IF EXISTS rotating_proxies ADD COLUMN IF NOT EXISTS instance_id varchar(191)`,
		`ALTER TABLE IF EXISTS rotating_proxies ADD COLUMN IF NOT EXISTS instance_name varchar(120)`,
		`ALTER TABLE IF EXISTS rotating_proxies ADD COLUMN IF NOT EXISTS instance_region varchar(120)`,
		`UPDATE rotating_proxies SET instance_id = ? WHERE COALESCE(instance_id, '') = ''`,
		`UPDATE rotating_proxies SET instance_name = instance_id WHERE COALESCE(instance_name, '') = ''`,
		`UPDATE rotating_proxies SET instance_region = 'Unknown' WHERE COALESCE(instance_region, '') = ''`,
		`ALTER TABLE IF EXISTS rotating_proxies ALTER COLUMN instance_id SET NOT NULL`,
		`ALTER TABLE IF EXISTS rotating_proxies ALTER COLUMN instance_name SET NOT NULL`,
		`ALTER TABLE IF EXISTS rotating_proxies ALTER COLUMN instance_region SET NOT NULL`,
		`DROP INDEX IF EXISTS idx_rotating_proxies_listen_port`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_rotating_instance_port ON rotating_proxies (instance_id, listen_port)`,
	}

	for _, stmt := range stmts {
		if strings.Contains(stmt, "?") {
			if err := db.Exec(stmt, currentInstanceID).Error; err != nil {
				return fmt.Errorf("rotating proxy schema: %w", err)
			}
			continue
		}
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("rotating proxy schema: %w", err)
		}
	}

	return nil
}

func EnsureProxyStatisticsRetentionSchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}
	if !db.Migrator().HasTable(&domain.ProxyStatistic{}) {
		return nil
	}

	if err := EnsureProxyStatisticsPartitionSchema(db); err != nil {
		return fmt.Errorf("proxy statistics partition schema: %w", err)
	}

	stmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_proxy_statistics_created_at_id ON proxy_statistics (created_at, id)`,
	}
	if db.Migrator().HasTable(&domain.ProxyLatestStatistic{}) {
		stmts = append(stmts, `CREATE INDEX IF NOT EXISTS idx_proxy_latest_statistics_statistic_id ON proxy_latest_statistics (statistic_id)`)
	}

	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("proxy statistics retention schema: %w", err)
		}
	}

	return nil
}

func ensureProxyTimelineRetentionSchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}

	stmts := make([]string, 0, 2)
	if db.Migrator().HasTable(&domain.ProxySnapshot{}) {
		stmts = append(stmts,
			`CREATE INDEX IF NOT EXISTS idx_proxy_snapshots_created_at_id ON proxy_snapshots (created_at, id)`,
			`CREATE INDEX IF NOT EXISTS idx_proxy_snapshots_workspace_metric_created_id ON proxy_snapshots (workspace_id, metric, created_at DESC, id DESC)`,
		)
	}
	if db.Migrator().HasTable(&domain.ProxyHistory{}) {
		stmts = append(stmts,
			`CREATE INDEX IF NOT EXISTS idx_proxy_histories_created_at_id ON proxy_histories (created_at, id)`,
			`CREATE INDEX IF NOT EXISTS idx_proxy_histories_workspace_created_id ON proxy_histories (workspace_id, created_at DESC, id DESC)`,
		)
	}

	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("proxy timeline retention schema: %w", err)
		}
	}

	return nil
}

func EnsureProxyQueryIndexSchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}

	stmts := make([]string, 0, 2)
	if db.Migrator().HasTable(&domain.UserProxy{}) {
		stmts = append(stmts, `CREATE INDEX IF NOT EXISTS idx_user_proxies_proxy_id ON user_proxies (proxy_id)`)
	}
	if db.Migrator().HasTable(&domain.ProxyStatistic{}) {
		stmts = append(stmts, `CREATE INDEX IF NOT EXISTS idx_proxy_statistics_proxy_created_id ON proxy_statistics (proxy_id, created_at DESC, id DESC)`)
	}
	if pg.IsPostgresDialect(db) {
		if err := db.Exec(`CREATE EXTENSION IF NOT EXISTS pg_trgm`).Error; err != nil {
			log.Warn("proxy query index schema: pg_trgm extension unavailable; search will use fallback indexes", "error", err)
		} else {
			if db.Migrator().HasTable(&domain.Proxy{}) {
				stmts = append(stmts,
					`CREATE INDEX IF NOT EXISTS idx_proxies_host_search_trgm ON proxies USING gin (LOWER(COALESCE(host, '')) gin_trgm_ops)`,
					`CREATE INDEX IF NOT EXISTS idx_proxies_country_search_trgm ON proxies USING gin (LOWER(COALESCE(NULLIF(country, ''), 'n/a')) gin_trgm_ops)`,
					`CREATE INDEX IF NOT EXISTS idx_proxies_estimated_type_search_trgm ON proxies USING gin (LOWER(COALESCE(NULLIF(estimated_type, ''), 'n/a')) gin_trgm_ops)`,
				)
			}
			if db.Migrator().HasTable(&domain.UserProxyFilterIndex{}) {
				stmts = append(stmts,
					`CREATE INDEX IF NOT EXISTS idx_user_proxy_filter_host_search_trgm ON user_proxy_filter_indexes USING gin (LOWER(COALESCE(host, '')) gin_trgm_ops)`,
				)
			}
			if db.Migrator().HasTable(&domain.ProxyReputation{}) {
				stmts = append(stmts,
					`CREATE INDEX IF NOT EXISTS idx_proxy_reputations_kind_search_trgm ON proxy_reputations USING gin (LOWER(COALESCE(kind, '')) gin_trgm_ops)`,
					`CREATE INDEX IF NOT EXISTS idx_proxy_reputations_label_search_trgm ON proxy_reputations USING gin (LOWER(COALESCE(label, '')) gin_trgm_ops)`,
				)
			}
		}
	}

	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("proxy query index schema: %w", err)
		}
	}

	return nil
}

func ShouldSkipProxyStatisticsAutoMigrate(db *gorm.DB) bool {
	if !pg.IsPostgresDialect(db) {
		return false
	}

	partitioned, err := IsProxyStatisticsPartitioned(db)
	if err != nil {
		log.Warn("Database migration: unable to detect proxy_statistics partition state", "error", err)
		return false
	}

	return partitioned
}

func RemoveMigrationModel(migrations []any, model any) []any {
	if len(migrations) == 0 {
		return migrations
	}

	target := reflect.TypeOf(model)
	if target == nil {
		return migrations
	}

	filtered := make([]any, 0, len(migrations))
	for _, migration := range migrations {
		if reflect.TypeOf(migration) == target {
			continue
		}
		filtered = append(filtered, migration)
	}
	return filtered
}

// MigrateModels prepares an empty database with the tables of every model:
// it configures the workspace join tables and auto-migrates the model list. It
// runs none of the preparation, post-migration or seed steps, which is the
// starting point tests expect.
func MigrateModels(db *gorm.DB) error {
	if err := ConfigureWorkspaceJoinTables(db); err != nil {
		return fmt.Errorf("configure workspace join tables: %w", err)
	}
	if err := db.AutoMigrate(DefaultMigrations()...); err != nil {
		return fmt.Errorf("auto migrate: %w", err)
	}
	return nil
}
