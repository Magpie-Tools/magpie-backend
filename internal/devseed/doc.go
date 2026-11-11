// Package devseed replaces the development fixture data of selected user
// accounts: their workspaces, routes, sources, measurements, timelines and
// alerts. cmd/seed-dev-data runs it against a local PostgreSQL database; it
// refuses production configurations.
//
// It sits above the core. SeedDevelopmentData opens one transaction on the
// handle it is given and calls core functions on that transaction (read-model
// refreshes, daily-check backfill marks, route queue changes).
package devseed
