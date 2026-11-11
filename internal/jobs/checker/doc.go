// Package checker runs the proxy checker workers. Each worker claims one
// route lease, sends the due workspaces' requests to judges, hands results
// to statistics ingestion, and waits for the shared failure-tracking and
// ownership batches before requeueing. A Runtime owns those batchers;
// app/bootstrap constructs one per process, runs its batchers, and stops them
// after the dispatcher has joined its workers.
//
// This is the hot path. Read the proxy checker rules in AGENTS.md before
// changing it: every change needs a written count of the cryptographic
// operations, JSON encodes, Redis commands and database queries per check.
package checker
