package pg

import (
	"strings"
)

func IsUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	normalized := strings.ToLower(err.Error())
	return strings.Contains(normalized, "duplicate key value violates unique constraint") ||
		strings.Contains(normalized, "unique constraint failed")
}
