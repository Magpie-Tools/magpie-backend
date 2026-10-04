---
status: accepted
---

# Resolve tag checker settings from runtime snapshots

The checker needs tag assignments to resolve settings, but an assignment query for each proxy would put database work into a loop that checks millions of routes. Keep checker rules and the assignments of tags with configured checker rules in memory, load them before affected checks run, and refresh them outside checker workers. Redis proxy payloads retain their existing purpose and ordinary requeues continue to update scheduling only.

Mutations must refresh the local snapshot after commit and notify other backend instances. Startup loading and reconciliation after missed notifications must restore an authoritative snapshot without a database fallback inside an individual check. This trades memory and mutation-time refresh work for zero additional cryptographic operations, JSON encodes, Redis commands, or database queries during per-check rule resolution; resolver CPU and memory costs still require measurement.

The implementation shares identical plans and keeps only route IDs whose
settings differ from Default. Published route maps use immutable shards; an edit
copies only the shards containing changed routes. This preserves snapshots held
by running checks without copying a million-entry map for a single tag change.

Each checker mutation advances a workspace revision. Assignment and rule changes
record the latest revision for each affected managed route in
`checker_proxy_changes`. The journal coalesces repeated edits. Ownership deletion
records a tombstone in the existing DELETE statement before assignments and
overrides cascade away. Tombstones belong to the workspace so missed deletion
notifications and reimports cannot restore a previous membership's checker plan.
Retention maintenance expires committed tombstones after 24 hours and advances
the full-refresh watermark before discarding recovery information. A process
older than that watermark reloads the complete snapshot; active instances keep
incremental refresh. Reimported memberships retain their journal entry. Workspace
deletion cascades journal rows. Default, judge, and SOCKS
judge-routing changes record a full-refresh revision. Cached readers compare
these watermarks and small configuration signatures before loading assignments.
An unchanged reconciliation reads no route data. Incremental refresh loads only
the journal's changed routes and their current checker-tag assignments, then
compares their committed SQL overrides. A missed change followed by a reversion
still records the route and repairs its projection, including across instances.

Startup and changes to shared configuration still require a complete load. This
trades a durable row per changed route, temporary removal tombstones, and
mutation-side revision writes for
bounded incremental work and reliable recovery from missed notifications. The
journal adds no checker-worker queries or queue payload writes.

Notifications refresh snapshots after commit; minute reconciliation handles
missed messages. A refresh failure preserves the last complete runtime snapshot
and marks health as pending in SQL until the projection succeeds. See
[measured lookup and snapshot costs](../performance/tag-checker-settings.md).
