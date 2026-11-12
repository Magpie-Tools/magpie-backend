// Package workspace owns workspaces and their memberships: creating and
// listing workspaces, resolving a user account's access and role, members and
// billing administrators, account-bound invitations, member preferences, and
// the capacity and allowance policy an administrator sets.
//
// It sits below the core: internal/database resolves workspace access through
// it, and it never imports internal/database. Usage and allowance accounting
// stay in the core, because they run inside its ingestion, lifecycle, cleanup
// and statistics transactions. The checker's workspace loader also stays there
// with the checker-settings SQL.
package workspace
