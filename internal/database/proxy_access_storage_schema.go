package database

import (
	"bytes"
	"fmt"
	"strings"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/gorm"
)

const proxyStorageMigrationBatchSize = 500

type legacyProxyStorageRow struct {
	ID                 uint64 `gorm:"column:id"`
	Port               uint16 `gorm:"column:port"`
	LegacyIP           string `gorm:"column:legacy_ip"`
	LegacyUsername     string `gorm:"column:legacy_username"`
	LegacyPassword     string `gorm:"column:legacy_password"`
	CurrentHost        string `gorm:"column:current_host"`
	CurrentIPAddress   string `gorm:"column:current_ip_address"`
	CurrentFingerprint []byte `gorm:"column:current_fingerprint"`
}

func ensureProxyAccessStorageSchema(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}
	if !isPostgresDialect(db) || !db.Migrator().HasTable(&domain.Proxy{}) {
		return nil
	}

	if !db.Migrator().HasColumn(&domain.Proxy{}, "host") || !db.Migrator().HasColumn(&domain.Proxy{}, "ip_address") {
		return fmt.Errorf("proxy access storage: host columns were not created")
	}
	if !db.Migrator().HasTable(&domain.UserProxy{}) ||
		!db.Migrator().HasColumn(&domain.UserProxy{}, "username") ||
		!db.Migrator().HasColumn(&domain.UserProxy{}, "password") {
		return fmt.Errorf("proxy access storage: credential columns were not created")
	}

	legacyIPExists := db.Migrator().HasColumn("proxies", "ip")
	legacyUsernameExists := db.Migrator().HasColumn("proxies", "username")
	legacyPasswordExists := db.Migrator().HasColumn("proxies", "password")

	if legacyIPExists {
		if err := migrateLegacyProxyStorage(db, legacyUsernameExists, legacyPasswordExists); err != nil {
			return err
		}
	}
	if err := db.Exec(`UPDATE proxies SET host = host(ip_address) WHERE (host IS NULL OR BTRIM(host) = '') AND ip_address IS NOT NULL`).Error; err != nil {
		return fmt.Errorf("proxy access storage: backfill route hosts: %w", err)
	}

	var missingHosts int64
	if err := db.Table("proxies").Where("host IS NULL OR BTRIM(host) = ''").Count(&missingHosts).Error; err != nil {
		return fmt.Errorf("proxy access storage: verify route hosts: %w", err)
	}
	if missingHosts != 0 {
		return fmt.Errorf("proxy access storage: %d routes have no host", missingHosts)
	}

	stmts := []string{
		`UPDATE user_proxies SET username = '' WHERE username IS NULL`,
		`UPDATE user_proxies SET password = '' WHERE password IS NULL`,
		`ALTER TABLE user_proxies ALTER COLUMN username SET DEFAULT ''`,
		`ALTER TABLE user_proxies ALTER COLUMN username SET NOT NULL`,
		`ALTER TABLE user_proxies ALTER COLUMN password SET DEFAULT ''`,
		`ALTER TABLE user_proxies ALTER COLUMN password SET NOT NULL`,
		`DROP INDEX IF EXISTS idx_proxy_addr`,
		`ALTER TABLE proxies ALTER COLUMN host SET NOT NULL`,
		`ALTER TABLE proxies ALTER COLUMN ip_address DROP NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_proxy_addr ON proxies (host, port)`,
		`CREATE INDEX IF NOT EXISTS idx_proxies_ip_address ON proxies (ip_address)`,
	}
	if legacyIPExists {
		stmts = append(stmts,
			`ALTER TABLE proxies DROP COLUMN IF EXISTS ip`,
			`ALTER TABLE proxies DROP COLUMN IF EXISTS ip_hash`,
			`ALTER TABLE proxies DROP COLUMN IF EXISTS ip_int`,
		)
	}
	if legacyUsernameExists {
		stmts = append(stmts, `ALTER TABLE proxies DROP COLUMN IF EXISTS username`)
	}
	if legacyPasswordExists {
		stmts = append(stmts, `ALTER TABLE proxies DROP COLUMN IF EXISTS password`)
	}

	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("proxy access storage: %w", err)
		}
	}

	return nil
}

func migrateLegacyProxyStorage(db *gorm.DB, hasUsername, hasPassword bool) error {
	usernameExpr := `''`
	if hasUsername {
		usernameExpr = `COALESCE(username, '')`
	}
	passwordExpr := `''`
	if hasPassword {
		passwordExpr = `COALESCE(password, '')`
	}

	var cursor uint64
	for {
		var rows []legacyProxyStorageRow
		query := fmt.Sprintf(`
			SELECT id,
			       port,
			       COALESCE(ip, '') AS legacy_ip,
			       %s AS legacy_username,
			       %s AS legacy_password,
			       COALESCE(proxies.host, '') AS current_host,
			       COALESCE(host(ip_address), '') AS current_ip_address,
			       hash AS current_fingerprint
			FROM proxies
			WHERE id > ?
			ORDER BY id
			LIMIT ?
		`, usernameExpr, passwordExpr)
		if err := db.Raw(query, cursor, proxyStorageMigrationBatchSize).Scan(&rows).Error; err != nil {
			return fmt.Errorf("proxy access storage: load legacy routes: %w", err)
		}
		if len(rows) == 0 {
			break
		}

		if err := db.Transaction(func(tx *gorm.DB) error {
			for _, row := range rows {
				if err := migrateLegacyProxyStorageRow(tx, row); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}

		cursor = rows[len(rows)-1].ID
	}

	return nil
}

func migrateLegacyProxyStorageRow(tx *gorm.DB, row legacyProxyStorageRow) error {
	routeHost := strings.TrimSpace(row.CurrentHost)
	if routeHost == "" {
		routeHost = strings.TrimSpace(row.CurrentIPAddress)
	}
	if routeHost == "" {
		plainIP, _, err := security.DecryptProxySecret(row.LegacyIP)
		if err != nil {
			return fmt.Errorf("proxy access storage: decrypt IP for route %d: %w", row.ID, err)
		}
		routeHost = plainIP
	}

	proxy := domain.Proxy{}
	if err := proxy.SetHost(routeHost); err != nil {
		return fmt.Errorf("proxy access storage: route %d has invalid host", row.ID)
	}
	routeHost = proxy.GetHost()
	ipAddress := proxy.GetIPAddress()

	password, _, err := security.DecryptProxySecret(row.LegacyPassword)
	if err != nil {
		return fmt.Errorf("proxy access storage: decrypt password for route %d: %w", row.ID, err)
	}
	fingerprint, err := security.FingerprintProxyRoute(routeHost, row.Port, row.LegacyUsername, password)
	if err != nil {
		return fmt.Errorf("proxy access storage: fingerprint route %d: %w", row.ID, err)
	}

	if routeHost != row.CurrentHost || ipAddress != row.CurrentIPAddress || !bytes.Equal(fingerprint, row.CurrentFingerprint) {
		var storedIPAddress any
		if ipAddress != "" {
			storedIPAddress = ipAddress
		}
		if err := tx.Exec(
			`UPDATE proxies SET host = ?, ip_address = ?::inet, hash = ? WHERE id = ?`,
			routeHost,
			storedIPAddress,
			fingerprint,
			row.ID,
		).Error; err != nil {
			return fmt.Errorf("proxy access storage: update route %d: %w", row.ID, err)
		}
	}

	usernameEncrypted, err := security.EncryptProxySecret(row.LegacyUsername)
	if err != nil {
		return fmt.Errorf("proxy access storage: encrypt username for route %d: %w", row.ID, err)
	}
	passwordEncrypted, err := security.EncryptProxySecret(password)
	if err != nil {
		return fmt.Errorf("proxy access storage: encrypt password for route %d: %w", row.ID, err)
	}

	if err := tx.Exec(
		`UPDATE user_proxies SET username = ?, password = ? WHERE proxy_id = ?`,
		usernameEncrypted,
		passwordEncrypted,
		row.ID,
	).Error; err != nil {
		return fmt.Errorf("proxy access storage: move credentials for route %d: %w", row.ID, err)
	}

	return nil
}
