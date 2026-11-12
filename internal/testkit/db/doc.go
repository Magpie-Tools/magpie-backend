// Package testdb is the shared database test kit. It opens migrated test
// databases and builds generic rows.
//
//   - OpenSQLite opens an in-memory SQLite database that belongs to one test.
//   - OpenPostgres opens an isolated PostgreSQL schema that belongs to one
//     test. Outside integration mode (MAGPIE_TEST_POSTGRES_DSN unset) it skips
//     the test; inside it, an unreachable server fails the test. A PostgreSQL
//     test never falls back to SQLite.
//   - QueryRecorder is a GORM logger that records each statement's SQL, for
//     tests that count or inspect the statements a function issues.
//
// Both migrate through platform/schema and set a proxy encryption key with a
// fresh cipher, so encrypted model hooks work.
//
// Only _test.go files may import this package; internal/archtest enforces
// that. A test that needs the shared pool handle pg.DB swaps it itself and
// restores it; the kit does not touch package-level state. Scenario fixtures
// stay with their tests until a second package needs them.
package testdb
