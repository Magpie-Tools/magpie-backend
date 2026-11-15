// Package dashboard serves the workspace dashboard: the info summary, recent
// checks and fastest alive routes. It sits above the core and reads current
// evidence, the read model and the usage meter; the core never imports it.
//
// ADR 0017 replaces the in-process cache kept here with shared,
// generation-stamped snapshot rows in PostgreSQL.
package dashboard
