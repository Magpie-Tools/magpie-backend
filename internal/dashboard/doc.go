// Package dashboard serves the workspace dashboard from shared,
// generation-stamped snapshot rows in PostgreSQL (ADR 0017).
//
// Any backend instance builds a workspace's snapshot on demand, after a read
// finds it missing, older than 30 seconds or without current health parts.
// A claim takes a lease epoch from a sequence and is admitted under a
// cluster-wide limit; publish and failure updates are fenced by the epoch.
// A build runs the existing dashboard queries, each in its own read-only
// transaction, under a deadline. Health parts are valid only for the checker
// generation they were built under and are masked while the workspace is
// dirty. The check figures are read live from the usage meter on every view.
//
// The package sits above the core and reads current evidence, the read model
// and the usage meter; the core never imports it.
package dashboard
