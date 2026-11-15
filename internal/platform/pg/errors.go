package pg

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func IsUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	normalized := strings.ToLower(err.Error())
	return strings.Contains(normalized, "duplicate key value violates unique constraint") ||
		strings.Contains(normalized, "unique constraint failed")
}

// IsLockNotAvailable reports PostgreSQL SQLSTATE 55P03, which a statement
// returns when lock_timeout expires before it acquires a lock.
func IsLockNotAvailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}
