// Package alerts owns alert rules, destinations, incidents and deliveries:
// reading and saving rules and destinations, evaluating rules from existing
// workspace measurements, recording incident transitions with their outbox
// records, and claiming and delivering those records. It also holds the
// evaluation and delivery routines that app/bootstrap starts.
//
// It sits below the core: internal/database may call it, and it never imports
// internal/database. It reads current evidence through internal/evidence.
// CloseAlertForConfiguration runs on its caller's transaction and neither
// commits nor rolls back.
package alerts
