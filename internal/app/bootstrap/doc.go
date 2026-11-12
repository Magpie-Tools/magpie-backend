// Package bootstrap is the composition root. It loads settings, opens the
// database, reconstructs the Redis queues, and starts and stops every
// background routine and listener in a fixed order.
package bootstrap
