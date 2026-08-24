package database

import (
	"database/sql"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestProxyAccessStorageKeepsCaseSensitiveCredentialsOnUserRelation(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-access-storage-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	db, err := gorm.Open(sqlite.Open("file:proxy-access-storage?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := configureWorkspaceJoinTables(db); err != nil {
		t.Fatalf("configure workspace join tables: %v", err)
	}
	if err := db.AutoMigrate(&domain.ManagedProxy{}); err != nil {
		t.Fatalf("migrate managed proxy schema: %v", err)
	}
	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Workspace{},
		&domain.WorkspaceMembership{},
		&domain.WorkspaceSubscription{},
		&domain.Proxy{},
	); err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	users := []domain.User{
		{Email: "first@example.test", Password: "hash", Role: "user"},
		{Email: "second@example.test", Password: "hash", Role: "user"},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("create users: %v", err)
	}
	for _, user := range users {
		createTestWorkspaceForUser(t, db, user)
	}

	first := domain.Proxy{Port: 8080, Username: "CaseUser", Password: "CaseSecret", Country: "N/A", EstimatedType: "N/A"}
	if err := first.SetIP("192.0.2.10"); err != nil {
		t.Fatalf("set first IP: %v", err)
	}
	firstInserted, err := InsertAndGetProxiesWithUser([]domain.Proxy{first}, users[0].ID)
	if err != nil {
		t.Fatalf("insert first proxy: %v", err)
	}
	if len(firstInserted) != 1 {
		t.Fatalf("inserted first proxy count = %d, want 1", len(firstInserted))
	}

	second := domain.Proxy{Port: 8080, Username: "caseuser", Password: "casesecret", Country: "N/A", EstimatedType: "N/A"}
	if err := second.SetIP("192.0.2.10"); err != nil {
		t.Fatalf("set second IP: %v", err)
	}
	secondInserted, err := InsertAndGetProxiesWithUser([]domain.Proxy{second}, users[1].ID)
	if err != nil {
		t.Fatalf("insert second proxy: %v", err)
	}
	if len(secondInserted) != 1 {
		t.Fatalf("inserted second proxy count = %d, want 1", len(secondInserted))
	}
	if firstInserted[0].ID == secondInserted[0].ID {
		t.Fatal("credential casing variants resolved to the same proxy route")
	}

	assertStoredProxyAccess(t, db, users[0].ID, firstInserted[0].ID, "CaseUser", "CaseSecret")
	assertStoredProxyAccess(t, db, users[1].ID, secondInserted[0].ID, "caseuser", "casesecret")

	if db.Migrator().HasColumn(&domain.Proxy{}, "username") || db.Migrator().HasColumn(&domain.Proxy{}, "password") {
		t.Fatal("proxy route table still contains credential columns")
	}

	var storedIP string
	if err := db.Table("proxies").Where("id = ?", firstInserted[0].ID).Pluck("ip_address", &storedIP).Error; err != nil {
		t.Fatalf("load stored IP: %v", err)
	}
	if storedIP != "192.0.2.10" {
		t.Fatalf("stored IP = %q, want native address", storedIP)
	}
}

func TestProxyAccessStoragePersistsIPv6Address(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-access-storage-ipv6-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	db, err := gorm.Open(sqlite.Open("file:proxy-access-storage-ipv6?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := configureWorkspaceJoinTables(db); err != nil {
		t.Fatalf("configure workspace join tables: %v", err)
	}
	if err := db.AutoMigrate(&domain.ManagedProxy{}); err != nil {
		t.Fatalf("migrate managed proxy schema: %v", err)
	}
	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Workspace{},
		&domain.WorkspaceMembership{},
		&domain.WorkspaceSubscription{},
		&domain.Proxy{},
	); err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	user := domain.User{Email: "ipv6@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	createTestWorkspaceForUser(t, db, user)

	proxy := domain.Proxy{Port: 8080, Country: "N/A", EstimatedType: "N/A"}
	if err := proxy.SetIP("2001:0db8::42"); err != nil {
		t.Fatalf("set IPv6 address: %v", err)
	}
	inserted, err := InsertAndGetProxiesWithUser([]domain.Proxy{proxy}, user.ID)
	if err != nil {
		t.Fatalf("insert IPv6 proxy: %v", err)
	}
	if len(inserted) != 1 {
		t.Fatalf("inserted proxy count = %d, want 1", len(inserted))
	}
	if got := inserted[0].GetIp(); got != "2001:db8::42" {
		t.Fatalf("stored IPv6 address = %q, want canonical address", got)
	}
	if got := inserted[0].GetFullProxy(); got != "[2001:db8::42]:8080" {
		t.Fatalf("stored proxy address = %q, want bracketed address", got)
	}
}

func TestProxyAccessStoragePersistsProviderHostname(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-access-storage-hostname-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	db, err := gorm.Open(sqlite.Open("file:proxy-access-storage-hostname?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := configureWorkspaceJoinTables(db); err != nil {
		t.Fatalf("configure workspace join tables: %v", err)
	}
	if err := db.AutoMigrate(&domain.ManagedProxy{}); err != nil {
		t.Fatalf("migrate managed proxy schema: %v", err)
	}
	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Workspace{},
		&domain.WorkspaceMembership{},
		&domain.WorkspaceSubscription{},
		&domain.Proxy{},
	); err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	user := domain.User{Email: "hostname@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	createTestWorkspaceForUser(t, db, user)

	proxy := domain.Proxy{Port: 3128, Country: "N/A", EstimatedType: "N/A"}
	if err := proxy.SetHost("Gateway.Provider.Example."); err != nil {
		t.Fatalf("set provider hostname: %v", err)
	}
	inserted, err := InsertAndGetProxiesWithUser([]domain.Proxy{proxy}, user.ID)
	if err != nil {
		t.Fatalf("insert hostname proxy: %v", err)
	}
	if len(inserted) != 1 {
		t.Fatalf("inserted proxy count = %d, want 1", len(inserted))
	}
	if got := inserted[0].GetHost(); got != "gateway.provider.example" {
		t.Fatalf("stored provider hostname = %q", got)
	}
	if got := inserted[0].GetFullProxy(); got != "gateway.provider.example:3128" {
		t.Fatalf("stored proxy address = %q", got)
	}

	var stored struct {
		Host      string         `gorm:"column:host"`
		IPAddress sql.NullString `gorm:"column:ip_address"`
	}
	if err := db.Table("proxies").Select("host", "ip_address").Where("id = ?", inserted[0].ID).Scan(&stored).Error; err != nil {
		t.Fatalf("load stored hostname projection: %v", err)
	}
	if stored.Host != "gateway.provider.example" || stored.IPAddress.Valid {
		t.Fatalf("stored host/IP projection = %q/%v, want hostname with null IP", stored.Host, stored.IPAddress)
	}
}

func assertStoredProxyAccess(t *testing.T, db *gorm.DB, userID uint, proxyID uint64, username, password string) {
	t.Helper()

	var access domain.UserProxy
	if err := db.Where("workspace_id = ? AND proxy_id = ?", userID, proxyID).First(&access).Error; err != nil {
		t.Fatalf("load proxy access: %v", err)
	}
	if access.Username != username || access.Password != password {
		t.Fatalf("loaded credentials = %q:%q, want %q:%q", access.Username, access.Password, username, password)
	}
	if !security.IsProxySecretEncrypted(access.UsernameEncrypted) {
		t.Fatalf("stored username is not encrypted: %q", access.UsernameEncrypted)
	}
	if !security.IsProxySecretEncrypted(access.PasswordEncrypted) {
		t.Fatalf("stored password is not encrypted: %q", access.PasswordEncrypted)
	}

	proxy, err := GetQueuedProxyForUser(userID, proxyID)
	if err != nil {
		t.Fatalf("load queued proxy: %v", err)
	}
	if proxy == nil {
		t.Fatal("queued proxy was not found")
	}
	if proxy.Username != username || proxy.Password != password {
		t.Fatalf("queued credentials = %q:%q, want %q:%q", proxy.Username, proxy.Password, username, password)
	}
}
