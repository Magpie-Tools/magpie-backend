// Package alerts owns alert rules and destinations: reading a workspace's
// rules, incidents and deliveries, saving and deleting rules and destinations,
// and closing a rule's open incident when its configuration changes.
//
// It sits below the core: internal/database may call it, and it never imports
// internal/database. CloseAlertForConfiguration runs on its caller's
// transaction and neither commits nor rolls back.
package alerts
