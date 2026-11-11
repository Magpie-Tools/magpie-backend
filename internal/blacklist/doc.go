// Package blacklist maintains the in-memory IP blacklist: it refreshes the
// configured sources, synchronizes instances through Redis and filters
// blacklisted routes out of imports.
package blacklist
