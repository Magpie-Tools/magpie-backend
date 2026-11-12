// Package scrapestore persists scrape sources and their subscriptions: saving
// and deleting sources, listing and exporting a workspace's sources, fetch
// modes, scrape results and orphaned sources, and the source tag rules applied
// to routes from each scrape.
//
// It sits above the core. Its transactions call core functions that run on
// the transaction they are given (tag validation, checker assignment marks,
// source-stat refreshes); internal/database never imports this package. Route
// associations, a source's detail with route health, and the listing of a
// source's routes stay in internal/database.
package scrapestore
