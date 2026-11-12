// Package readmodel owns the derived read models of the proxy list and the
// scrape sources: the per-workspace proxy filter index
// (user_proxy_filter_indexes) and the per-workspace scrape-source counts
// (user_scrape_source_stats). It projects them from current checker evidence,
// either inside the caller's transaction (the Refresh* functions that take a
// *gorm.DB, which the core calls from statistics ingestion, route lifecycle,
// checker settings and source changes) or asynchronously from routes queued
// with QueueReadModelRefreshForProxyIDs, which StartReadModelRefreshRoutine
// projects in bounded, ordered rounds.
//
// The package sits below internal/database and never imports it. Its schema
// DDL is platform/schema.EnsureReadModelSchema; EnsureReadModelBackfill fills
// empty read models at startup.
//
// Tests in other packages finish all pending read-model work through the
// public shutdown path: StartReadModelRefreshRoutine given a context that has
// already ended drains every queued projection and source refresh and returns.
package readmodel
