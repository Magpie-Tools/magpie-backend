---
status: accepted
---

# Scope managed-proxy health to effective checker settings

Workspaces can check the same proxy route with different protocols, transports, timeouts, retries, and judge validation, so one shared latest result cannot describe every managed proxy's health. Assess each managed proxy using evidence attributable to its workspace and current effective checker settings, keeping obsolete results as history. This adds attribution and filtering work to health storage, but prevents another workspace or an incompatible transport from qualifying a proxy as alive.

Current rotating listeners connect upstream over TCP and must require matching TCP evidence. Existing statistics cannot be reliably backfilled with transport or settings attribution; current health remains unknown until applicable checks supply evidence. Settings identity is established when checker configuration changes, without introducing cryptographic fingerprint calculation into individual checks.

Each physical event keeps one history row with workspace verdict attribution.
Latest pointers are keyed by workspace, configuration key, route, and protocol.
Sparse tag projections and workspace default keys select applicable evidence.
Attributed events replace the former redundant global latest/overall writes;
unattributed legacy writers remain compatible during migration. Old history is
retained, and obsolete pointers are pruned outside checker workers.

The extra evidence serialization and latest-row fanout are necessary to preserve
independent validation and budgets. [Persistence measurements](../performance/tag-checker-settings.md)
record the operation count and the substantial throughput cost when a physical
result serves eight workspaces. This cost belongs to asynchronous persistence,
not tag lookup or queue requeue. Release load tests must size statistics workers
for the expected sharing level.

Reconciliation uses durable workspace and route revisions to select affected
routes, then compares their newly compiled plans with committed sparse overrides.
It repairs a missed change and reversion even when the local snapshot already
matches the final settings. Only changed routes and their subscribed sources are
rewritten; classification tags outside checker rules do not invalidate health.
Unchanged cached reconciliation skips assignment and sparse-projection reads.
Startup and shared configuration changes still compare the full projection.

Candidate health queries correlate route and workspace before aggregation.
Dashboard recent checks first select their limited candidates from the indexed
health projection, then fetch current evidence for those candidates. A partial
index orders active workspace routes by health, check time, and route ID. This
keeps evidence aggregates bounded by the requested limit even for a workspace
with millions of routes.
Dashboard cache entries carry the committed checker generation. Refreshes check
that generation before querying and before publishing, and cache reads verify it
against PostgreSQL so another instance's changes invalidate stale entries.
Read models retain evidence matching committed keys while a newer edit is
pending. Public readers mask pending health until projection commits.
