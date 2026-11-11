// Package pg holds the generic PostgreSQL helpers and all production
// connection creation: the GORM pool with its pool configuration, the
// connection of the read-model rebuild command with its cancellation
// behaviour, and the native pgx connection used for COPY.
//
// DB is the shared pool handle that database.SetupDB assigns. It is the one
// handle every package runs on until its callers receive a handle explicitly,
// and it disappears with its last caller.
//
// The package knows no table and no feature. Other packages create no
// database connection; internal/archtest enforces that.
package pg
