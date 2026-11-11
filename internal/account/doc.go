// Package account owns user account persistence that belongs to no workspace:
// looking up a user account, changing its password, password-reset tokens,
// and the per-account receipts of read notifications.
//
// Registration, settings and account deletion stay where their transactions
// are: deleting an account removes workspace resources, so it lives in
// internal/database. DeletePasswordResetTokensForUser runs on its caller's
// transaction and neither commits nor rolls back.
package account
