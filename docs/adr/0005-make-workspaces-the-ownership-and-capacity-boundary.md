# Make workspaces the ownership and capacity boundary

Magpie uses a workspace as its resource, collaboration, quota, and subscription boundary. Users gain access through memberships. The workspace owns managed proxies, tags, judges, scrape sources, rotators, operational settings, capacity, usage, and its subscription. Capacity cannot be contributed or withdrawn by members. Every existing user receives a personal workspace during migration, and all user-owned resources move to that workspace so Magpie has one ownership path.

## Considered options

We rejected keeping personal resources beside shared resources because every authorization and quota path would need two owners. We rejected member-contributed proxy allowances because a member leaving could remove production capacity, create unclear overage responsibility, and let free accounts pool allowances. We also reserved "organization" for a future parent that can consolidate billing and SSO across workspaces, and "team" for a future permission group inside a workspace.

## Consequences

One workspace-to-route association is one managed proxy and consumes one active-route unit while active. Paused and archived managed proxies remain stored but do not enter check or rotator queues. Reaching capacity blocks new activations or uses a configured paid-overage allowance; it never deletes routes. Membership removal changes access only. Billing integration may arrive later, but plan entitlements, purchased capacity, usage periods, provider references, and spend policy belong to workspace subscription and capacity records from the start.
