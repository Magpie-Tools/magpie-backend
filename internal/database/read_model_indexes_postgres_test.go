package database

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestReadModelBackfillStoresHostAndLiteralIPProjection(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("MAGPIE_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN is not set")
	}

	t.Setenv("PROXY_ENCRYPTION_KEY", "read-model-hostname-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	schema := fmt.Sprintf("read_model_hostname_test_%d", time.Now().UnixNano())
	if err := admin.Exec(`CREATE SCHEMA ` + schema).Error; err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error
	})

	db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		t.Fatalf("open schema database: %v", err)
	}
	if err := db.AutoMigrate(defaultMigrations()...); err != nil {
		t.Fatalf("auto migrate schema: %v", err)
	}
	if err := ensureProxyAccessStorageSchema(db); err != nil {
		t.Fatalf("finalize proxy host schema: %v", err)
	}

	user := domain.User{Email: "read-model-hostname@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	ipRoute := domain.Proxy{Port: 8080, Country: "N/A", EstimatedType: "N/A"}
	if err := ipRoute.SetIP("2001:db8::80"); err != nil {
		t.Fatalf("set IP route: %v", err)
	}
	hostnameRoute := domain.Proxy{Port: 3128, Country: "N/A", EstimatedType: "N/A"}
	if err := hostnameRoute.SetHost("Gateway.Provider.Example."); err != nil {
		t.Fatalf("set hostname route: %v", err)
	}
	proxies := []domain.Proxy{ipRoute, hostnameRoute}
	if err := db.Create(&proxies).Error; err != nil {
		t.Fatalf("create proxies: %v", err)
	}
	if err := db.Create(&[]domain.UserProxy{
		{WorkspaceID: user.ID, ProxyID: proxies[0].ID},
		{WorkspaceID: user.ID, ProxyID: proxies[1].ID},
	}).Error; err != nil {
		t.Fatalf("create proxy access rows: %v", err)
	}

	if err := ensureReadModelBackfill(db); err != nil {
		t.Fatalf("backfill read model: %v", err)
	}

	var rows []struct {
		Host      string  `gorm:"column:host"`
		IPAddress *string `gorm:"column:ip_address"`
	}
	if err := db.Table("user_proxy_filter_indexes").Select("host", "ip_address").Order("proxy_id").Scan(&rows).Error; err != nil {
		t.Fatalf("load read model rows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("read model row count = %d, want 2", len(rows))
	}
	if rows[0].Host != "2001:db8::80" || rows[0].IPAddress == nil || *rows[0].IPAddress != "2001:db8::80" {
		t.Fatalf("IP route projection = %#v", rows[0])
	}
	if rows[1].Host != "gateway.provider.example" || rows[1].IPAddress != nil {
		t.Fatalf("hostname route projection = %#v", rows[1])
	}
}
