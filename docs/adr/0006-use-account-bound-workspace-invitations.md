# Use account-bound workspace invitations

Magpie represents a workspace invitation as a time-limited offer attached to an existing user account. The invitation is owned by the workspace, identifies the recipient by user ID, and grants no access until that authenticated account accepts it. It does not contain a secret or bearer token. Email is an optional notification through the durable outbox and links only to the signed-in `/invitations` inbox.

## Considered options

We rejected immediate membership creation because it grants access without recipient consent and gives recipients no place to decline. We rejected tokenized public invitation links because forwarded or leaked tokens would transfer the offer, require another secret lifecycle, and complicate account matching. We rejected making email delivery authoritative because self-hosted instances may intentionally omit SMTP and outbox failures should not erase a valid pending offer. We also rejected retaining invitation history because the current product needs only live access offers and has no audit-log contract.

## Decision

Only existing Magpie accounts can be invited. One pending row may exist for a workspace and recipient account. Owners can offer admin, operator, or viewer access and billing administration; admins are limited to operator or viewer without billing access. The row retains an inviter email snapshot and a nullable inviter foreign key so it remains manageable if the inviter leaves.

Acceptance creates membership and deletes the invitation in one database transaction without changing the recipient's default workspace. Decline and revoke delete the row. A leader-coordinated hourly job deletes expired rows. The default lifetime is seven days and is self-host configurable, but editing access never renews the expiry.

When SMTP is configured, creation queues an invitation email and an actual role or billing change queues an “invitation changed” email. Queue failure is reported to the sender but does not roll back the row. There is no resend endpoint, send toggle, or notification on acceptance, decline, revoke, or expiry.

## Consequences

Pending access is discoverable in the product even without email, and a forwarded email cannot be used by another account. The design gives up pre-registration invitations and historical invitation reporting. If Magpie later needs external onboarding or compliance audit logs, those must be introduced as separate concepts rather than weakening the account-bound offer or treating the transient invitation table as an audit record.
