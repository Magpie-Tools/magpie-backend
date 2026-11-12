package domain

import "strings"

// NormalizeEmail returns the canonical form of a user account's email address,
// which login, invitations and membership lookups compare.
func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}
