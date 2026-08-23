package database

import (
	"bytes"
	"crypto/sha256"
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

func TestEnsureProxyAccessStorageSchemaMigratesLegacyPostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("MAGPIE_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN is not set")
	}

	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-storage-postgres-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	schema := fmt.Sprintf("proxy_access_storage_test_%d", time.Now().UnixNano())
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
	createLegacyProxyStorageSchema(t, db)

	ipCiphertext, err := security.EncryptProxySecret("198.51.100.23")
	if err != nil {
		t.Fatalf("encrypt legacy IP: %v", err)
	}
	passwordCiphertext, err := security.EncryptProxySecret("CaseSecret")
	if err != nil {
		t.Fatalf("encrypt legacy password: %v", err)
	}
	legacyHash := sha256.Sum256([]byte(strings.ToLower("198.51.100.23|8080|CaseUser|CaseSecret")))

	if err := db.Exec(`
		INSERT INTO proxies (id, ip, port, username, password, country, estimated_type, hash, created_at)
		VALUES (1, ?, 8080, 'CaseUser', ?, 'DE', 'residential', ?, CURRENT_TIMESTAMP)
	`, ipCiphertext, passwordCiphertext, legacyHash[:]).Error; err != nil {
		t.Fatalf("insert legacy proxy: %v", err)
	}
	if err := db.Exec(`
		INSERT INTO user_proxies (user_id, proxy_id, consecutive_failures, created_at)
		VALUES (7, 1, 0, CURRENT_TIMESTAMP)
	`).Error; err != nil {
		t.Fatalf("insert legacy user proxy: %v", err)
	}

	if err := db.AutoMigrate(&domain.Proxy{}, &domain.UserProxy{}); err != nil {
		t.Fatalf("auto migrate new storage columns: %v", err)
	}
	if err := ensureProxyAccessStorageSchema(db); err != nil {
		t.Fatalf("migrate legacy proxy storage: %v", err)
	}

	var proxy domain.Proxy
	if err := db.First(&proxy, 1).Error; err != nil {
		t.Fatalf("load migrated proxy: %v", err)
	}
	if proxy.GetIp() != "198.51.100.23" {
		t.Fatalf("migrated IP = %q, want 198.51.100.23", proxy.GetIp())
	}
	expectedFingerprint, err := security.FingerprintProxyRoute("198.51.100.23", 8080, "CaseUser", "CaseSecret")
	if err != nil {
		t.Fatalf("calculate expected fingerprint: %v", err)
	}
	if !bytes.Equal(proxy.Hash, expectedFingerprint) {
		t.Fatal("migrated route fingerprint does not match exact credentials")
	}

	var access domain.UserProxy
	if err := db.Where("user_id = 7 AND proxy_id = 1").First(&access).Error; err != nil {
		t.Fatalf("load migrated access: %v", err)
	}
	if access.Username != "CaseUser" || access.Password != "CaseSecret" {
		t.Fatalf("migrated credentials = %q:%q", access.Username, access.Password)
	}

	for _, legacyColumn := range []string{"ip", "ip_hash", "ip_int", "username", "password"} {
		if db.Migrator().HasColumn("proxies", legacyColumn) {
			t.Errorf("legacy proxy column %q still exists", legacyColumn)
		}
	}

	var dataType string
	if err := db.Raw(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = 'proxies' AND column_name = 'ip_address'
	`, schema).Scan(&dataType).Error; err != nil {
		t.Fatalf("load IP column type: %v", err)
	}
	if dataType != "inet" {
		t.Fatalf("IP column type = %q, want inet", dataType)
	}

	var hostDataType string
	if err := db.Raw(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = 'proxies' AND column_name = 'host'
	`, schema).Scan(&hostDataType).Error; err != nil {
		t.Fatalf("load host column type: %v", err)
	}
	if hostDataType != "character varying" {
		t.Fatalf("host column type = %q, want character varying", hostDataType)
	}

	var storedHost string
	if err := db.Table("proxies").Where("id = 1").Pluck("host", &storedHost).Error; err != nil {
		t.Fatalf("load migrated host: %v", err)
	}
	if storedHost != "198.51.100.23" {
		t.Fatalf("migrated host = %q, want 198.51.100.23", storedHost)
	}

	var ipAddressNullable string
	if err := db.Raw(`
		SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = 'proxies' AND column_name = 'ip_address'
	`, schema).Scan(&ipAddressNullable).Error; err != nil {
		t.Fatalf("load IP column nullability: %v", err)
	}
	if ipAddressNullable != "YES" {
		t.Fatalf("IP column nullability = %q, want YES for hostname routes", ipAddressNullable)
	}

	hostnameRoute := domain.Proxy{ID: 2, Port: 3128, Country: "N/A", EstimatedType: "N/A"}
	if err := hostnameRoute.SetHost("Gateway.Provider.Example."); err != nil {
		t.Fatalf("set hostname route: %v", err)
	}
	if err := db.Create(&hostnameRoute).Error; err != nil {
		t.Fatalf("insert hostname route: %v", err)
	}
	var hostnameProjection struct {
		Host      string  `gorm:"column:host"`
		IPAddress *string `gorm:"column:ip_address"`
	}
	if err := db.Table("proxies").Select("host", "ip_address").Where("id = ?", hostnameRoute.ID).Scan(&hostnameProjection).Error; err != nil {
		t.Fatalf("load hostname route projection: %v", err)
	}
	if hostnameProjection.Host != "gateway.provider.example" || hostnameProjection.IPAddress != nil {
		t.Fatalf("hostname route projection = %q/%v", hostnameProjection.Host, hostnameProjection.IPAddress)
	}

	var nullableCredentialColumns int64
	if err := db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = ?
		  AND table_name = 'user_proxies'
		  AND column_name IN ('username', 'password')
		  AND is_nullable = 'YES'
	`, schema).Scan(&nullableCredentialColumns).Error; err != nil {
		t.Fatalf("load credential nullability: %v", err)
	}
	if nullableCredentialColumns != 0 {
		t.Fatalf("nullable credential columns = %d, want 0", nullableCredentialColumns)
	}
}

func TestEnsureProxyAccessStorageSchemaBackfillsCurrentIPAddressRoutes(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("MAGPIE_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN is not set")
	}

	// The current IP-only schema does not need the encryption key to gain the
	// host projection. Its existing route fingerprint must remain untouched.
	t.Setenv("PROXY_ENCRYPTION_KEY", "")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	schema := fmt.Sprintf("proxy_host_backfill_test_%d", time.Now().UnixNano())
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
	createCurrentIPAddressProxyStorageSchema(t, db)

	originalHash := sha256.Sum256([]byte("current-ip-route-fingerprint"))
	if err := db.Exec(`
		INSERT INTO proxies (id, ip_address, port, country, estimated_type, hash, created_at)
		VALUES (1, '2001:db8::23'::inet, 8080, 'DE', 'datacenter', ?, CURRENT_TIMESTAMP)
	`, originalHash[:]).Error; err != nil {
		t.Fatalf("insert current IP route: %v", err)
	}

	if err := db.AutoMigrate(&domain.Proxy{}); err != nil {
		t.Fatalf("auto migrate hostname columns: %v", err)
	}
	if err := ensureProxyAccessStorageSchema(db); err != nil {
		t.Fatalf("backfill current IP route: %v", err)
	}

	var row struct {
		Host        string `gorm:"column:host"`
		IPAddress   string `gorm:"column:ip_address"`
		Fingerprint []byte `gorm:"column:hash"`
	}
	if err := db.Raw(`
		SELECT host, host(ip_address) AS ip_address, hash
		FROM proxies
		WHERE id = 1
	`).Scan(&row).Error; err != nil {
		t.Fatalf("load backfilled route: %v", err)
	}
	if row.Host != "2001:db8::23" || row.IPAddress != "2001:db8::23" {
		t.Fatalf("backfilled host/IP = %q/%q", row.Host, row.IPAddress)
	}
	if !bytes.Equal(row.Fingerprint, originalHash[:]) {
		t.Fatal("current route fingerprint changed during host backfill")
	}

	var indexDefinition string
	if err := db.Raw(`
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = ? AND indexname = 'idx_proxy_addr'
	`, schema).Scan(&indexDefinition).Error; err != nil {
		t.Fatalf("load route index definition: %v", err)
	}
	if !strings.Contains(indexDefinition, "(host, port)") {
		t.Fatalf("route index = %q, want host and port", indexDefinition)
	}
}

func createLegacyProxyStorageSchema(t *testing.T, db *gorm.DB) {
	t.Helper()

	statements := []string{
		`CREATE TABLE proxies (
			id bigserial PRIMARY KEY,
			ip text NOT NULL DEFAULT '',
			ip_hash bytea,
			ip_int bigint NOT NULL DEFAULT 0,
			port integer NOT NULL,
			username text NOT NULL DEFAULT '',
			password text NOT NULL DEFAULT '',
			country varchar(56) NOT NULL,
			estimated_type varchar(20) NOT NULL,
			hash bytea NOT NULL,
			created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE UNIQUE INDEX idx_proxies_hash ON proxies (hash)`,
		`CREATE INDEX idx_proxy_addr ON proxies (ip, port)`,
		`CREATE TABLE user_proxies (
			user_id bigint NOT NULL,
			proxy_id bigint NOT NULL,
			consecutive_failures integer NOT NULL DEFAULT 0,
			created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (user_id, proxy_id)
		)`,
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("create legacy schema: %v", err)
		}
	}
}

func createCurrentIPAddressProxyStorageSchema(t *testing.T, db *gorm.DB) {
	t.Helper()

	statements := []string{
		`CREATE TABLE proxies (
			id bigserial PRIMARY KEY,
			ip_address inet NOT NULL,
			port integer NOT NULL,
			country varchar(56) NOT NULL,
			estimated_type varchar(20) NOT NULL,
			hash bytea NOT NULL,
			created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE UNIQUE INDEX idx_proxies_hash ON proxies (hash)`,
		`CREATE INDEX idx_proxy_addr ON proxies (ip_address, port)`,
		`CREATE TABLE user_proxies (
			user_id bigint NOT NULL,
			proxy_id bigint NOT NULL,
			username text NOT NULL DEFAULT '',
			password text NOT NULL DEFAULT '',
			consecutive_failures integer NOT NULL DEFAULT 0,
			created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (user_id, proxy_id)
		)`,
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("create current IP storage schema: %v", err)
		}
	}
}
