// Package app is the process entry point: it parses flags, runs the
// migrate-only and rebuild-read-models commands, and otherwise constructs the
// process's checkpace.Pacer and queues and hands control to bootstrap and the
// API server.
package app
