// Package proxyqueue is the Redis proxy queue: compact route payloads, due
// times in sharded sorted sets, and worker leases.
//
// This is the hot path. Read the proxy checker rules in AGENTS.md before
// changing it. A normal requeue updates only sorted-set scheduling state and
// a current plaintext payload reuses its stored hash without the cipher.
package proxyqueue
