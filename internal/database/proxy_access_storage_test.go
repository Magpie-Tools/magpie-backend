package database

import (
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
	if err := db.AutoMigrate(&domain.User{}, &domain.Proxy{}, &domain.UserProxy{}); err != nil {
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

func assertStoredProxyAccess(t *testing.T, db *gorm.DB, userID uint, proxyID uint64, username, password string) {
	t.Helper()

	var access domain.UserProxy
	if err := db.Where("user_id = ? AND proxy_id = ?", userID, proxyID).First(&access).Error; err != nil {
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
