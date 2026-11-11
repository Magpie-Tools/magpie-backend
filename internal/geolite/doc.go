// Package geolite owns the GeoLite databases and the local route metadata they
// produce: downloading the databases and distributing them between instances
// through Redis, loading them (from disk, or the embedded fallback at
// startup), enriching routes with country and proxy type, and the periodic geo
// refresh with its update routine.
//
// It sits above the core: it writes route metadata through
// database.UpdateProxyMetadataBatch on the shared pool handle and reads the
// provider overrides of internal/database's ipgeolocation, which stays in the
// core. The core never imports this package.
package geolite
