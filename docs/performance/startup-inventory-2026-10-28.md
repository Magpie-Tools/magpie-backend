# Select each startup route without expanding shared owners

The current preflight failed its projection SQL client's 15-second deadline
after 15109.939 seconds of catch-up. Peak and last eligible matches were
928/1024. The startup observer recorded four errors; the backend failed its
read-model drain and exited 1. These remain failed acceptance evidence.
Recovery, smoke and the load gate did not start.

Retained startup activity includes 105 active samples of the inventory page
query, with a maximum observed age of 13.072 seconds. The query joins active
ownership before applying DISTINCT to every physical-route column. Eight
active owners can produce eight copies of a route for deduplication. This
observation identifies repeated read work; it does not establish that this
query alone caused the later timeout or drain failure.

ForEachProxyBatch now uses an active-ownership EXISTS predicate. A physical
route's primary key already makes its row unique, so the membership test
preserves the previous selected rows without expanding all matching owners.
The existing keyset order, page size, callback copies, active-workspace
hydration and credential hydration remain unchanged. Paused, archived and
unowned-only routes remain excluded; a route with any active owner is included.
Ownership changes between pages retain statement-by-statement visibility.

## Stopped-fixture comparison

One read-only repeatable-read transaction compared a page after route 500000
on the million-route/eight-workspace fixture. Each arm executed once. Both
returned the same complete 2000-row fingerprint, IDs 500001 through 502000.
Compile-only plans show the original incremental sort and two Unique nodes;
the candidate uses a nested-loop semi join without that deduplication.

The complete-row fingerprint SELECT took 1183.674 ms for the original and
18.196 ms for the candidate. The original ran first, and the candidate inherited
warmer pages and potentially different autovacuum activity. These durations
do not establish a causal timing reduction or live capacity. No persistent data
was written and no live validation budget, resource limit or PostgreSQL setting
was changed. Raw SQL, plans and results are retained in
.codex-tmp/release-remediation-20261020/takeover-20261025/noop-deps-failure-20261028/startup-inventory-comparison/.

## Validation and operation cost

Seven selected groups passed normal in 6.487 seconds and race in 12.955 seconds,
without skips. New cases run on SQLite and PostgreSQL and cover shared routes,
mixed owner lifecycle states, complete route fields, stored hashes, hydrated
credentials/settings, keyset page boundaries, current ownership transfers,
retained callback batches, cancellation and callback errors. Both owned
PostgreSQL test services stopped cleanly. These are functional durations.

Only the startup iterator's route-selection statement changes production
source. Its command count is unchanged, and both hydration calls remain in
the same place. No encryption, decryption, nonce, route hash, JSON encoding,
Redis command, credential load or database query is added to a default
physical proxy check. The queue payload, plaintext default and stable key
remain unchanged. Fresh full checks for this source and the unchanged strict
catch-up, representative recovery, corrected smoke and entire 25-hour load
gate remain mandatory.
