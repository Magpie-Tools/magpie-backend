// Package database is the persistence core: proxy routes and managed
// proxies, check statistics and latest evidence, read models, dashboards,
// rotators, checker settings and workspace usage accounting. Peripheral
// features are being extracted into their own packages in stages (see
// docs/adr/0015-extract-feature-packages-behind-architecture-tests.md); the
// code that remains shares transactions and stays together.
//
// New code belongs here only when it must run inside one of this package's
// transactions. internal/archtest limits this package's size and importers.
package database
