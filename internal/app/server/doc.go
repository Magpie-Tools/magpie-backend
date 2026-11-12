// Package server holds the REST transport: the central route table, request
// handlers and their authorization checks. Handlers call feature packages and
// the core database package; feature packages never import this package.
package server
