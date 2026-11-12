package pg

import (
	"strings"

	"gorm.io/gorm"
)

func IsPostgresDialect(db *gorm.DB) bool {
	if db == nil {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(db.Dialector.Name()))
	return strings.Contains(name, "postgres")
}

func IsPostgresDialector(db *gorm.DB) bool {
	if db == nil || db.Dialector == nil {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(db.Dialector.Name()))
	return strings.Contains(name, "postgres")
}

func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
