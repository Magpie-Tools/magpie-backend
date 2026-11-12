// Package blacklist maintains the IP blacklist: it refreshes the configured
// sources, synchronizes instances through Redis, filters blacklisted routes
// out of imports, stores the lists (a native COPY when PostgreSQL is
// available) and removes managed routes whose address became blacklisted.
//
// It sits above the core: route removal calls database.DeleteProxyRelation,
// which owns its transactions. The core never imports this package. The
// website blocklist cleanup stays in internal/database.
package blacklist
