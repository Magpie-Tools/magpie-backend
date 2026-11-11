// Package schema holds the ordered schema steps and migrations: the model list
// for auto-migration, the preparation and post-migration steps, partition
// maintenance helpers and seed rows.
//
// Every step takes the handle it runs on. The package knows no feature worker
// and starts nothing; the caller decides the order of DDL and projection
// backfills.
package schema
