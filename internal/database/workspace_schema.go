package database

import (
	"fmt"
	"strings"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// These migration-only rows create the workspace tables without asking GORM
// to traverse resource relationships before their legacy ownership columns
// have been renamed.
type workspaceMigrationRow struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	Name     string `gorm:"not null;size:120"`
	Personal bool   `gorm:"not null;default:false;index"`

	HTTPProtocol               bool   `gorm:"not null;default:false"`
	HTTPSProtocol              bool   `gorm:"not null;default:true"`
	SOCKS4Protocol             bool   `gorm:"not null;default:false"`
	SOCKS5Protocol             bool   `gorm:"not null;default:false"`
	Timeout                    uint16 `gorm:"not null;default:7500"`
	Retries                    uint8  `gorm:"not null;default:2"`
	UseHttpsForSocks           bool   `gorm:"not null;default:true"`
	TransportProtocol          string `gorm:"not null;default:'tcp'"`
	AutoRemoveFailingProxies   bool   `gorm:"not null;default:false"`
	AutoRemoveFailureThreshold uint8  `gorm:"not null;default:3"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (workspaceMigrationRow) TableName() string { return "workspaces" }

type workspaceMembershipMigrationRow struct {
	WorkspaceID  uint   `gorm:"primaryKey"`
	UserID       uint   `gorm:"primaryKey;index"`
	Role         string `gorm:"not null;size:16"`
	BillingAdmin bool   `gorm:"not null;default:false"`
	IsDefault    bool   `gorm:"not null;default:false"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (workspaceMembershipMigrationRow) TableName() string { return "workspace_memberships" }

type workspacePreferenceMigrationRow struct {
	WorkspaceID              uint              `gorm:"primaryKey"`
	UserID                   uint              `gorm:"primaryKey;index"`
	ProxyListColumns         domain.StringList `gorm:"type:jsonb;default:'[]'"`
	ScrapeSourceProxyColumns domain.StringList `gorm:"type:jsonb;default:'[]'"`
	ScrapeSourceListColumns  domain.StringList `gorm:"type:jsonb;default:'[]'"`
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

func (workspacePreferenceMigrationRow) TableName() string { return "workspace_member_preferences" }

type workspaceSubscriptionMigrationRow struct {
	WorkspaceID                 uint   `gorm:"primaryKey"`
	PlanCode                    string `gorm:"not null;size:64;default:'self-hosted'"`
	Status                      string `gorm:"not null;size:24;default:'active'"`
	IncludedActiveRoutes        uint64 `gorm:"not null;default:0"`
	AdditionalActiveRoutes      uint64 `gorm:"not null;default:0"`
	OverageMode                 string `gorm:"not null;size:16;default:'unlimited'"`
	OverageActiveRoutes         uint64 `gorm:"not null;default:0"`
	IncludedOperators           uint32 `gorm:"not null;default:0"`
	StatisticsRetentionDays     uint32 `gorm:"not null;default:0"`
	MinimumCheckIntervalSeconds uint32 `gorm:"not null;default:0"`
	BillingProvider             string `gorm:"size:32;default:''"`
	ProviderCustomerID          string `gorm:"size:191;default:''"`
	ProviderSubscriptionID      string `gorm:"size:191;default:''"`
	CurrentPeriodStart          *time.Time
	CurrentPeriodEnd            *time.Time
	CancelAtPeriodEnd           bool `gorm:"not null;default:false"`
	CreatedAt                   time.Time
	UpdatedAt                   time.Time
}

func (workspaceSubscriptionMigrationRow) TableName() string { return "workspace_subscriptions" }

type managedProxyMigrationRow struct {
	WorkspaceID         uint   `gorm:"column:workspace_id;primaryKey;index:idx_managed_proxies_workspace_state,priority:1"`
	ProxyID             uint64 `gorm:"primaryKey;index:idx_user_proxies_proxy_id"`
	UsernameEncrypted   string `gorm:"column:username;default:''"`
	PasswordEncrypted   string `gorm:"column:password;default:''"`
	ConsecutiveFailures uint16 `gorm:"not null;default:0"`
	State               string `gorm:"not null;size:16;default:'active';index:idx_managed_proxies_workspace_state,priority:2"`
	PauseReason         string `gorm:"not null;size:24;default:''"`
	ActivatedAt         *time.Time
	PausedAt            *time.Time
	ArchivedAt          *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (managedProxyMigrationRow) TableName() string { return "user_proxies" }

var workspaceOwnedTables = []string{
	"user_proxies",
	"proxy_tags",
	"proxy_tag_assignments",
	"user_proxy_filter_indexes",
	"user_scrape_source_stats",
	"rotating_proxies",
	"proxy_histories",
	"proxy_snapshots",
	"user_judges",
	"user_scrape_site",
}

func configureWorkspaceJoinTables(db *gorm.DB) error {
	registrations := []struct {
		model       any
		association string
		join        any
	}{
		{model: &domain.Proxy{}, association: "Workspaces", join: &domain.ManagedProxy{}},
		{model: &domain.Workspace{}, association: "Proxies", join: &domain.ManagedProxy{}},
		{model: &domain.ScrapeSite{}, association: "Workspaces", join: &domain.WorkspaceScrapeSite{}},
		{model: &domain.Workspace{}, association: "ScrapeSites", join: &domain.WorkspaceScrapeSite{}},
		{model: &domain.Judge{}, association: "Workspaces", join: &domain.WorkspaceJudge{}},
		{model: &domain.Workspace{}, association: "Judges", join: &domain.WorkspaceJudge{}},
	}
	for _, registration := range registrations {
		if err := db.SetupJoinTable(registration.model, registration.association, registration.join); err != nil {
			return err
		}
	}
	return nil
}

func prepareWorkspaceOwnershipMigration(db *gorm.DB) error {
	if db == nil || !db.Migrator().HasTable(&domain.User{}) {
		return nil
	}

	if err := db.AutoMigrate(
		&workspaceMigrationRow{},
		&workspaceMembershipMigrationRow{},
		&workspacePreferenceMigrationRow{},
		&workspaceSubscriptionMigrationRow{},
	); err != nil {
		return fmt.Errorf("create workspace migration tables: %w", err)
	}

	if err := seedPersonalWorkspaces(db); err != nil {
		return err
	}

	for _, table := range workspaceOwnedTables {
		if !db.Migrator().HasTable(table) || !db.Migrator().HasColumn(table, "user_id") || db.Migrator().HasColumn(table, "workspace_id") {
			continue
		}
		if err := db.Migrator().RenameColumn(table, "user_id", "workspace_id"); err != nil {
			return fmt.Errorf("rename %s.user_id to workspace_id: %w", table, err)
		}
	}

	return nil
}

func seedPersonalWorkspaces(db *gorm.DB) error {
	legacyOwnershipPresent := false
	for _, table := range workspaceOwnedTables {
		if db.Migrator().HasTable(table) && db.Migrator().HasColumn(table, "user_id") {
			legacyOwnershipPresent = true
			break
		}
	}

	var users []domain.User
	if err := db.Select(
		"id", "email", "role",
		"http_protocol", "http_s_protocol", "socks4_protocol", "socks5_protocol",
		"timeout", "retries", "use_https_for_socks", "transport_protocol",
		"auto_remove_failing_proxies", "auto_remove_failure_threshold",
		"proxy_list_columns", "scrape_source_proxy_columns", "scrape_source_list_columns",
	).Order("id").Find(&users).Error; err != nil {
		return fmt.Errorf("load users for personal workspace migration: %w", err)
	}

	for _, user := range users {
		var existingMemberships int64
		if err := db.Model(&workspaceMembershipMigrationRow{}).
			Where("user_id = ?", user.ID).
			Count(&existingMemberships).Error; err != nil {
			return fmt.Errorf("count workspace memberships for user %d: %w", user.ID, err)
		}
		if existingMemberships > 0 {
			continue
		}

		workspace := workspaceMigrationRow{
			Name:                       personalWorkspaceName(user.Email),
			Personal:                   true,
			HTTPProtocol:               user.HTTPProtocol,
			HTTPSProtocol:              user.HTTPSProtocol,
			SOCKS4Protocol:             user.SOCKS4Protocol,
			SOCKS5Protocol:             user.SOCKS5Protocol,
			Timeout:                    user.Timeout,
			Retries:                    user.Retries,
			UseHttpsForSocks:           user.UseHttpsForSocks,
			TransportProtocol:          user.TransportProtocol,
			AutoRemoveFailingProxies:   user.AutoRemoveFailingProxies,
			AutoRemoveFailureThreshold: user.AutoRemoveFailureThreshold,
		}
		// The one-time ownership rename preserves user_id values, so those
		// legacy rows need an identically numbered workspace. Once ownership
		// columns have migrated, workspace and account sequences are independent.
		if legacyOwnershipPresent {
			workspace.ID = user.ID
		}
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&workspace).Error; err != nil {
			return fmt.Errorf("create personal workspace for user %d: %w", user.ID, err)
		}
		if legacyOwnershipPresent {
			var stored workspaceMigrationRow
			if err := db.First(&stored, user.ID).Error; err != nil {
				return fmt.Errorf("load personal workspace for user %d: %w", user.ID, err)
			}
			if !stored.Personal {
				return fmt.Errorf("workspace id %d is already used by a non-personal workspace", user.ID)
			}
			workspace = stored
		}

		membership := workspaceMembershipMigrationRow{
			WorkspaceID:  workspace.ID,
			UserID:       user.ID,
			Role:         domain.WorkspaceRoleOwner,
			BillingAdmin: true,
			IsDefault:    true,
		}
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&membership).Error; err != nil {
			return fmt.Errorf("create personal workspace membership for user %d: %w", user.ID, err)
		}

		preference := workspacePreferenceMigrationRow{
			WorkspaceID:              workspace.ID,
			UserID:                   user.ID,
			ProxyListColumns:         user.ProxyListColumns,
			ScrapeSourceProxyColumns: user.ScrapeSourceProxyColumns,
			ScrapeSourceListColumns:  user.ScrapeSourceListColumns,
		}
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&preference).Error; err != nil {
			return fmt.Errorf("create personal workspace preferences for user %d: %w", user.ID, err)
		}

		subscription := defaultWorkspaceSubscription(user.Role)
		subscription.WorkspaceID = workspace.ID
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&subscription).Error; err != nil {
			return fmt.Errorf("create personal workspace subscription for user %d: %w", user.ID, err)
		}
	}

	if isPostgresDialect(db) {
		if err := db.Exec(`SELECT setval(pg_get_serial_sequence('workspaces', 'id'), GREATEST(COALESCE((SELECT MAX(id) FROM workspaces), 1), 1), true)`).Error; err != nil {
			return fmt.Errorf("advance workspace id sequence: %w", err)
		}
	}

	return nil
}

func ensureWorkspaceSchema(db *gorm.DB) error {
	if db == nil || !db.Migrator().HasTable(&domain.Workspace{}) {
		return nil
	}

	if err := seedPersonalWorkspaces(db); err != nil {
		return err
	}

	if !isPostgresDialect(db) {
		return nil
	}
	if err := ensureWorkspaceOwnershipForeignKeys(db); err != nil {
		return err
	}

	stmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_workspace_memberships_user_default ON workspace_memberships (user_id, workspace_id, is_default)`,
		`CREATE INDEX IF NOT EXISTS idx_managed_proxies_workspace_state ON user_proxies (workspace_id, state)`,
		`CREATE INDEX IF NOT EXISTS idx_proxy_tags_workspace_name ON proxy_tags (workspace_id, name_key)`,
		`CREATE INDEX IF NOT EXISTS idx_proxy_snapshots_workspace_metric_created_id ON proxy_snapshots (workspace_id, metric, created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proxy_histories_workspace_created_id ON proxy_histories (workspace_id, created_at DESC, id DESC)`,
	}
	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("workspace schema: %w", err)
		}
	}
	return nil
}

func ensureWorkspaceOwnershipForeignKeys(db *gorm.DB) error {
	// PostgreSQL preserves a foreign key's referenced table when its local
	// column is renamed. Drop those legacy users(id) constraints and replace
	// them with workspace ownership constraints after every legacy account has
	// been seeded with a personal workspace.
	const statement = `
DO $$
DECLARE
	table_name text;
	constraint_name text;
	desired_constraint text;
BEGIN
	FOREACH table_name IN ARRAY ARRAY[
		'user_proxies',
		'proxy_tags',
		'proxy_tag_assignments',
		'user_proxy_filter_indexes',
		'user_scrape_source_stats',
		'rotating_proxies',
		'proxy_histories',
		'proxy_snapshots',
		'user_judges',
		'user_scrape_site'
	]
	LOOP
		IF to_regclass(current_schema() || '.' || table_name) IS NULL THEN
			CONTINUE;
		END IF;

		FOR constraint_name IN
			SELECT con.conname
			FROM pg_constraint con
			JOIN pg_class source_table ON source_table.oid = con.conrelid
			JOIN pg_class target_table ON target_table.oid = con.confrelid
			JOIN pg_namespace source_namespace ON source_namespace.oid = source_table.relnamespace
			WHERE con.contype = 'f'
			  AND source_namespace.nspname = current_schema()
			  AND source_table.relname = table_name
			  AND target_table.relname = 'users'
		LOOP
			EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', table_name, constraint_name);
		END LOOP;

		desired_constraint := 'fk_' || table_name || '_workspace';
		IF NOT EXISTS (
			SELECT 1
			FROM pg_constraint con
			JOIN pg_class source_table ON source_table.oid = con.conrelid
			JOIN pg_namespace source_namespace ON source_namespace.oid = source_table.relnamespace
			WHERE con.contype = 'f'
			  AND source_namespace.nspname = current_schema()
			  AND source_table.relname = table_name
			  AND con.conname = desired_constraint
		) THEN
			EXECUTE format(
				'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON UPDATE CASCADE ON DELETE CASCADE',
				table_name,
				desired_constraint
			);
		END IF;
	END LOOP;
END $$;
`
	if err := db.Exec(statement).Error; err != nil {
		return fmt.Errorf("workspace ownership foreign keys: %w", err)
	}
	return nil
}

func personalWorkspaceName(email string) string {
	name := strings.TrimSpace(email)
	if at := strings.IndexByte(name, '@'); at > 0 {
		name = name[:at]
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "Personal"
	}
	return name + "'s workspace"
}

func defaultWorkspaceSubscription(globalRole string) domain.WorkspaceSubscription {
	limits := config.GetConfig().ProxyLimits
	subscription := domain.WorkspaceSubscription{
		PlanCode:    "self-hosted",
		Status:      domain.WorkspaceSubscriptionStatusActive,
		OverageMode: domain.WorkspaceOverageUnlimited,
	}
	if limits.Enabled && !(limits.ExcludeAdmins && globalRole == "admin") {
		subscription.IncludedActiveRoutes = uint64(limits.MaxPerUser)
		subscription.OverageMode = domain.WorkspaceOverageDisabled
	}
	return subscription
}
