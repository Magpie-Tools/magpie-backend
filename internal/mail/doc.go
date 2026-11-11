// Package mail owns the email outbox and its delivery: enqueueing a message,
// claiming and retrying pending messages, the delivery and maintenance
// routines, and retention of sent and abandoned messages.
//
// Handlers enqueue; app/bootstrap starts the routines. The package still runs
// on the core's shared handle, database.DB.
package mail
