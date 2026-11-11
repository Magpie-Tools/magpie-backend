// Package evidence builds the SQL that reads a workspace's current checker
// evidence: the latest statistics that match its effective checker settings,
// overall status joins, alive-route counts, and the complete pool-eligibility
// query of a rotator with its cache key.
//
// It sits below the core and below alerts. Every function takes the handle or
// transaction of its caller; the package holds no handle, begins no transaction
// and issues no statement the caller did not ask for.
package evidence
