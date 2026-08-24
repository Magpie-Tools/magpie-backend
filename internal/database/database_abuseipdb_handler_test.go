package database

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestGetNextAbuseIPDBProxyToCheckSkipsHostnameRoutes(t *testing.T) {
	t.Setenv("PROXY_ENCRYPTION_KEY", "abuseipdb-hostname-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	db, err := gorm.Open(sqlite.Open("file:abuseipdb-hostname?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := db.AutoMigrate(&domain.User{}, &domain.Proxy{}, &domain.UserProxy{}, &domain.AbuseIPDBCheck{}); err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	user := domain.User{Email: "abuse-hostname@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	hostnameProxy := domain.Proxy{Port: 8080, Country: "N/A", EstimatedType: "N/A"}
	if err := hostnameProxy.SetHost("gateway.provider.example"); err != nil {
		t.Fatalf("SetHost: %v", err)
	}
	ipProxy := domain.Proxy{Port: 8080, Country: "N/A", EstimatedType: "N/A"}
	if err := ipProxy.SetIP("192.0.2.80"); err != nil {
		t.Fatalf("SetIP: %v", err)
	}
	proxies := []domain.Proxy{hostnameProxy, ipProxy}
	if err := db.Create(&proxies).Error; err != nil {
		t.Fatalf("create proxies: %v", err)
	}
	if err := db.Create(&[]domain.UserProxy{
		{WorkspaceID: user.ID, ProxyID: proxies[0].ID},
		{WorkspaceID: user.ID, ProxyID: proxies[1].ID},
	}).Error; err != nil {
		t.Fatalf("create proxy access rows: %v", err)
	}

	selected, err := GetNextAbuseIPDBProxyToCheck(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("select AbuseIPDB proxy: %v", err)
	}
	if selected == nil || selected.GetIPAddress() != "192.0.2.80" {
		t.Fatalf("selected proxy = %#v, want literal IP route", selected)
	}
}
