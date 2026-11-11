// Package pg holds the generic PostgreSQL helpers and all production
// connection creation: the GORM pool with its pool configuration, the
// connection of the read-model rebuild command with its cancellation
// behaviour, and the native pgx connection used for COPY.
//
// It knows no table and no feature. Other packages create no database
// connection; internal/archtest enforces that.
package pg
