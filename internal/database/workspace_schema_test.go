package database

import (
	"fmt"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type legacyWorkspaceMigrationUser struct {
	ID                         uint              `gorm:"primaryKey;autoIncrement"`
	Email                      string            `gorm:"not null;size:255"`
	Password                   string            `gorm:"not null;size:100"`
	Role                       string            `gorm:"not null;default:'user';check:role IN ('user', 'admin')"`
	HTTPProtocol               bool              `gorm:"not null;default:false"`
	HTTPSProtocol              bool              `gorm:"not null;default:true"`
	SOCKS4Protocol             bool              `gorm:"not null;default:false"`
	SOCKS5Protocol             bool              `gorm:"not null;default:false"`
	Timeout                    uint16            `gorm:"not null;default:7500"`
	Retries                    uint8             `gorm:"not null;default:2"`
	UseHttpsForSocks           bool              `gorm:"not null;default:true"`
	TransportProtocol          string            `gorm:"not null;default:'tcp'"`
	AutoRemoveFailingProxies   bool              `gorm:"not null;default:false"`
	AutoRemoveFailureThreshold uint8             `gorm:"not null;default:3"`
	ProxyListColumns           domain.StringList `gorm:"type:jsonb;default:'[]'"`
	ScrapeSourceProxyColumns   domain.StringList `gorm:"type:jsonb;default:'[]'"`
	ScrapeSourceListColumns    domain.StringList `gorm:"type:jsonb;default:'[]'"`
	CreatedAt                  time.Time         `gorm:"autoCreateTime"`
}

func (legacyWorkspaceMigrationUser) TableName() string { return "users" }

type legacyWorkspaceMigrationProxy struct {
	ID            uint64    `gorm:"primaryKey;autoIncrement"`
	IP            string    `gorm:"column:host;type:varchar(253);index:idx_proxy_addr,priority:1"`
	IPAddress     *string   `gorm:"column:ip_address;type:inet;index"`
	Port          uint16    `gorm:"not null;index:idx_proxy_addr,priority:2"`
	Country       string    `gorm:"size:56;not null"`
	EstimatedType string    `gorm:"size:20;not null"`
	Hash          []byte    `gorm:"type:bytea;uniqueIndex;size:32"`
	CreatedAt     time.Time `gorm:"autoCreateTime"`
}

func (legacyWorkspaceMigrationProxy) TableName() string { return "proxies" }

type legacyWorkspaceMigrationManagedProxy struct {
	UserID              uint      `gorm:"primaryKey"`
	ProxyID             uint64    `gorm:"primaryKey;index:idx_user_proxies_proxy_id"`
	Username            string    `gorm:"default:''"`
	Password            string    `gorm:"default:''"`
	ConsecutiveFailures uint16    `gorm:"not null;default:0"`
	CreatedAt           time.Time `gorm:"autoCreateTime"`

	User  legacyWorkspaceMigrationUser  `gorm:"foreignKey:UserID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
	Proxy legacyWorkspaceMigrationProxy `gorm:"foreignKey:ProxyID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
}

func (legacyWorkspaceMigrationManagedProxy) TableName() string { return "user_proxies" }

func TestSetupDBMigratesLegacyUserOwnershipToPersonalWorkspace(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "workspace-migration-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", t.Name())
	legacyDB, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	if err := legacyDB.AutoMigrate(
		&legacyWorkspaceMigrationUser{},
		&legacyWorkspaceMigrationProxy{},
		&legacyWorkspaceMigrationManagedProxy{},
	); err != nil {
		t.Fatalf("migrate legacy schema: %v", err)
	}

	legacyUser := legacyWorkspaceMigrationUser{
		Email:                    "existing@example.com",
		Password:                 "password123",
		Role:                     "user",
		HTTPSProtocol:            true,
		Timeout:                  4200,
		Retries:                  4,
		UseHttpsForSocks:         true,
		TransportProtocol:        "tcp",
		ProxyListColumns:         domain.StringList{"ip_port", "country"},
		ScrapeSourceProxyColumns: domain.StringList{"ip_port"},
		ScrapeSourceListColumns:  domain.StringList{"url"},
	}
	if err := legacyDB.Create(&legacyUser).Error; err != nil {
		t.Fatalf("create legacy user: %v", err)
	}
	legacyProxy := legacyWorkspaceMigrationProxy{
		ID:        91,
		IP:        "192.0.2.91",
		Port:      8091,
		Hash:      []byte("legacy-route-hash"),
		CreatedAt: time.Now().UTC(),
	}
	if err := legacyDB.Create(&legacyProxy).Error; err != nil {
		t.Fatalf("create legacy proxy: %v", err)
	}
	if err := legacyDB.Create(&legacyWorkspaceMigrationManagedProxy{
		UserID:    legacyUser.ID,
		ProxyID:   legacyProxy.ID,
		Username:  "legacy-user",
		Password:  "legacy-pass",
		CreatedAt: time.Now().UTC(),
	}).Error; err != nil {
		t.Fatalf("create legacy ownership: %v", err)
	}

	previousDB := DB
	t.Cleanup(func() { DB = previousDB })
	migratedDB, err := SetupDB(func(cfg *Config) {
		cfg.ExistingDB = legacyDB
		cfg.Dialector = nil
		cfg.AutoMigrate = true
		cfg.SeedDefaults = false
		cfg.Migrations = defaultMigrations()
	})
	if err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	var workspace domain.Workspace
	if err := migratedDB.First(&workspace, legacyUser.ID).Error; err != nil {
		t.Fatalf("load personal workspace: %v", err)
	}
	if !workspace.Personal || workspace.Timeout != legacyUser.Timeout || workspace.Retries != legacyUser.Retries {
		t.Fatalf("personal workspace did not retain user settings: %#v", workspace)
	}

	var membership domain.WorkspaceMembership
	if err := migratedDB.Where("workspace_id = ? AND user_id = ?", workspace.ID, legacyUser.ID).First(&membership).Error; err != nil {
		t.Fatalf("load owner membership: %v", err)
	}
	if membership.Role != domain.WorkspaceRoleOwner || !membership.BillingAdmin || !membership.IsDefault {
		t.Fatalf("unexpected migrated membership: %#v", membership)
	}

	var managed domain.ManagedProxy
	if err := migratedDB.Where("workspace_id = ? AND proxy_id = ?", workspace.ID, 91).First(&managed).Error; err != nil {
		t.Fatalf("load migrated managed proxy: %v", err)
	}
	if managed.State != domain.ManagedProxyStateActive {
		t.Fatalf("migrated lifecycle state = %q, want active", managed.State)
	}

	shared, err := CreateWorkspace(legacyUser.ID, "Shared production")
	if err != nil {
		t.Fatalf("create shared workspace after migration: %v", err)
	}
	if shared.ID == legacyUser.ID {
		t.Fatal("shared workspace unexpectedly reused personal workspace id")
	}

	newUser := domain.User{Email: "post-migration@example.com", Password: "password123", Role: "user"}
	if err := migratedDB.Create(&newUser).Error; err != nil {
		t.Fatalf("create post-migration user: %v", err)
	}
	newPersonal, err := CreatePersonalWorkspaceForUser(migratedDB, newUser)
	if err != nil {
		t.Fatalf("create post-migration personal workspace: %v", err)
	}
	if newUser.ID != shared.ID {
		t.Fatalf("test setup did not create the intended id collision: user=%d shared=%d", newUser.ID, shared.ID)
	}
	if newPersonal.ID == newUser.ID {
		t.Fatalf("post-migration personal workspace reused account id %d", newUser.ID)
	}

	if err := seedPersonalWorkspaces(migratedDB); err != nil {
		t.Fatalf("rerun personal workspace seeding: %v", err)
	}
	var postMigrationMemberships []domain.WorkspaceMembership
	if err := migratedDB.Where("user_id = ?", newUser.ID).Find(&postMigrationMemberships).Error; err != nil {
		t.Fatalf("load post-migration memberships: %v", err)
	}
	if len(postMigrationMemberships) != 1 || postMigrationMemberships[0].WorkspaceID != newPersonal.ID {
		t.Fatalf("post-migration memberships = %#v, want only workspace %d", postMigrationMemberships, newPersonal.ID)
	}
}
