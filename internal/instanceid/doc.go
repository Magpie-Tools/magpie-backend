// Package instanceid resolves the stable identifier of this backend
// instance from configuration, a persisted file or a scoped fallback, and
// counts the live instances in the Redis heartbeat index (CountActiveInstances),
// which the proxy and scrape-source queues use to size their workers.
package instanceid
