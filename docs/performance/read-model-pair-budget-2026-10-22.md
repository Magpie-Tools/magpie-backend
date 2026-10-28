# Projection batches at shared-workspace scale

The retained release fixture exposed a projection backlog after the statistics
consumer had drained. At 05:05 UTC on 22 October, the stream had no pending events
or lag, but route 3 had successful current evidence from 01:35:58 for all eight
owners and a proxy-list `latest_check` still dated 20 October. The projection
worker repeatedly canceled batches of 5,000 physical routes at its 30-second
query deadline. The backend subsequently exited with an unfinished drain. This
was a failed readiness run.

A physical-route budget did not bound projection writes. Five thousand shared
routes expanded to 40,000 workspace/route rows, each with 23 physical projection
indexes. The SQL also allowed PostgreSQL to join the selected scope against the
whole proxy metadata table. The earlier regression checked latest-evidence
visits but had an empty reputation table and did not check metadata visits.

## SQL and worker changes

Projection SQL now obtains ownership, metadata, reputation, checker plans and
latest evidence through parameterized lookups. Metadata is materialized once
per distinct physical route in each statement. The ownership lookup rechecks
current ownership, state and pause reason instead of trusting previously
expanded values. No index is added.

The effective `READ_MODEL_REFRESH_BATCH_SIZE` is a workspace/route pair budget.
It defaults to 5,000 and positive values above 5,000 are clamped to 5,000. The
existing numeric defaults remain four prefixes per pass, a five-second ticker
and a 30-second context for each prefix. Each pair contributes two PostgreSQL
parameters, below the extended-protocol limit.

The worker discovers owners for at most 5,000 dirty physical routes, then retains
an unfinished pair suffix between passes. A successful prefix remains committed
when a later prefix fails. Source invalidation follows completion of all owners
in that physical-route batch. A source-discovery failure retains the source-only
phase without repeating completed projection statements. Invalidations that
arrive while a suffix is pending remain in the physical dirty map and receive a
new ownership discovery afterward. A flush mutex serializes direct concurrent
flush invocations as well as the single production worker.

Owner discovery still expands the actual owner fanout into memory. The selected
fixture produced 40,000 pair keys from 5,000 routes and eight owners. This is not
a constant-memory guarantee for arbitrary workspace counts. The hard projection
statement limit applies to actual ownership pairs, not to that discovery result.

A same-statement `IS DISTINCT FROM` guard compares all 26 mutable projected
values. Identical content preserves `updated_at` and avoids rewriting the
projection indexes. Changed timestamps from real evidence, lifecycle state,
metadata, nullable health and reputation still update the row. Conflict checks
still lock rows and can generate WAL. PostgreSQL documents this distinction in
[the INSERT reference](https://www.postgresql.org/docs/17/sql-insert.html).

Current-key, override, dirty-workspace and generation-zero behavior remains the
same. The pair budget bounds projected rows and ownership/metadata lookups.
Latest-evidence work also depends on the number of protocols and retained config
keys in each selected namespace. The measured fixture has one latest row per
pair; it does not establish a universal limit of 5,000 latest-evidence visits.

## Explicit repair after a failed stop

Dirty notifications and the retained suffix are process memory. An unclean stop
can lose them while leaving a nonempty stale projection. Ordinary startup
backfill only covers an empty projection or missing hosts, so an empty dirty map
after restart does not establish consistency.

`database.RefreshReadModelIndexes(ctx)` now performs an explicit repair using
production projection queries. Run it against an existing migrated schema with
application writers stopped. It pages authoritative ownership by the primary
key, at most 5,000 pairs per projection query, and source ownership in pages of
100. It never changes inventory, credentials, checker configuration or durable
check evidence. The CLI exposes this helper separately from runtime startup.

Repair also removes orphan derived rows. Authoritative ownership removal and
derived deletion were separate operations, so a crash could leave a publicly
visible proxy-list row without `user_proxies` ownership. Cleanup first pages the
derived primary key, then probes ownership for only those selected keys. It does
not apply an antijoin to the full table before `LIMIT`. The delete consumes TIDs
inside the same statement and never stores them across pages. These derived
tables are not partitioned. The stopped-writer requirement remains necessary;
this repair does not change the preexisting in-statement race with concurrent
ownership deletion during ordinary runtime.

Cancellation, failed discovery, failed projection or failed source repair returns
an error. Earlier committed prefixes remain durable. A rerun starts keyset
pagination again and completes the remainder; unchanged projections do not
rewrite their indexes. Repair success requires completion of both cleanup and
refresh phases. Source aggregation still reads members of each selected source;
a page of 100 source pairs does not bound the size of an individual source.

## Retained-fixture measurements

The backend was stopped for these statements. Each used a local 15-second
statement deadline and two-second lock deadline inside `BEGIN` / `ROLLBACK`.
No `VACUUM`, `ANALYZE`, schema, resource or configuration change ran on this
fixture. Rollback preserved logical contents. The diagnostic upserts still
produced WAL, cache changes and aborted row versions, so later trials do not
represent identical physical cache state.

| Statement | Actual projected/scope rows | Proxy metadata visits | SQL time |
| --- | ---: | ---: | ---: |
| Previous SQL, SELECT computation for 5,000 physical routes | 40,000 | 994,803 | 11.159 s |
| Parameterized metadata only, same SELECT computation | 40,000 | 5,000 | 10.073 s |
| Previous 5,000-physical-route upsert | 40,000 | Not available after cancellation | Hit 15 s cap |
| Final SQL, one owner and 5,000 physical routes | 5,000 | 5,000 | 8.011 s |
| Final SQL, eight owners and 625 physical routes | 5,000 | 625 | 8.443 s |

The final two statements each read 5,000 ownership rows and 5,000 latest rows.
Reputation visits were respectively 5,000 and 625. They produced 234.1 MB and
292.5 MB of WAL before rollback. Metadata-only alternatives still hit the cap
when asked to update all 40,000 pairs. Both parameterized reads and pair-based
write limits are needed.

The actual captured SQL, plans, deadlines and results are in
`.codex-tmp/release-remediation-20261020/projection-refresh-20261022/actual-quiet/`
in the workspace. `measure-actual.py` sends SQL through stdin to avoid shell
argument-size limits.

## Complete populated fixture

The isolated PostgreSQL fixture uses four CPUs, 4 GiB RAM, an 8 GiB combined
memory/swap ceiling, 128 MB `shared_buffers` and 4 MB `work_mem`. PostgreSQL
reports version 17.11. The exact image digest, startup arguments and observed
settings are recorded in `fixture-resources.json`.

Before measured writes it contains one million physical routes, eight million
ownership rows, eight million current latest rows, eight million existing
projection rows and one million populated reputation rows. All 23 production
projection indexes exist. Fixture loading follows `pg_restore`: secondary
indexes and five original foreign keys are restored and validated before timing.
Measurements run without application/dashboard/checker traffic.

The first complete run passed in 275.219 seconds, including 205.005 seconds of
fixture setup. Owner discovery expanded 5,000 physical routes into 40,000 keys in
173 ms. Eight prefixes each visited only the selected 5,000 ownership, proxy,
reputation and latest rows. Initial content updates took 1.60–2.50 seconds per
prefix; fresh indexed `latest_check` updates took 1.41–5.04 seconds. Identical
retries took 0.57–1.69 seconds. The changed trials generated 383.9 MB of WAL in
total and the unchanged trials 25.0 MB, including checkpoint-dependent full-page
images.

The previous 40,000-row SQL finished in 16.329 seconds on this quiet synthetic
fixture. The eight changed-prefix SQL times totaled 18.238 seconds. These
measurements demonstrate bounded statements and retained partial progress, not
an increase in total write throughput. Production readiness still requires the
coordinated restore and the full fixed-profile soak.

The final repeat passed in 258.744 seconds, including 201.368 seconds of setup.
Owner discovery took 214 ms. Initial content updates took 1.61–2.45 seconds per
prefix, fresh indexed updates 1.26–2.15 seconds, and unchanged retries
0.48–0.80 seconds. The old complete SQL took 7.612 seconds in this repeat,
while the eight fresh indexed prefixes totaled 14.028 seconds. Cache and
checkpoint state materially affect these measurements; a throughput improvement
is not claimed.

The final fixture also checks orphan-cleanup plans at the beginning, middle and
end of the eight-million-row projection. Each candidate page visited exactly
5,000 derived rows. Each delete probed 5,000 ownership rows, 5,000 selected
derived rows and one orphan TID, taking 11.50–18.02 ms. With 80,000 source
ownership/projection rows, each source page visited 100 keys, and deletes probed
100 owners and 101 derived/TID rows in 0.24–0.27 ms. No candidate or delete plan
scanned a whole table. The 12 candidate/delete plans and exact SQL are under
`projection-refresh-20261022/million-final/` in the remediation evidence folder.

## Operation counts and verification

The checker notification function still only inserts stored physical IDs into a
coalescing map. This change adds zero cryptographic operations, route hashes,
JSON encodes, Redis commands, credential loads or direct database queries to
queue dequeue/check/requeue. Plaintext queue defaults, stored hashes and normal
sorted-set-only requeues are unchanged.

Background database cost increases to bound individual writes. For 5,000 routes
shared by eight owners, the previous path used one projection table check, one
40,000-row upsert, one source table check and one source-pair lookup. The new path
adds one owner discovery and uses eight projection table checks and eight
5,000-row upserts, followed by the same two source statements. That is 19
asynchronous statements instead of four, excluding implicit transaction control
and the separate source aggregation worker. These are coalesced batch costs,
not direct checker round trips. A smaller regression measures the same formula:
one physical route, eight owners and a pair budget of two produce four prefixes,
five table checks and two discovery statements, 11 statements total. Queueing
the notification itself performs zero SQL queries.

Focused regressions cover retained prefix progress, concurrent flushes, new
invalidations/new owners during a suffix, committed ownership removal before a
future prefix, source-only retries, canceled discovery, locked projection rows,
shutdown retry and effective parameter caps. Explicit-repair coverage includes
cancellation after a committed orphan-cleanup prefix, failure after partial
refresh, retry, already-projected proxy/source orphans, missing derived tables
and an unchanged raw inventory/credential/config/evidence fingerprint. The
populated-evidence regression now fills reputation and asserts physical metadata
visits as well as latest-evidence visits.

Reproduce the scale fixture from `magpie-backend`:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_read_model_test password=magpie_read_model_test dbname=magpie_read_model_test sslmode=disable' \
MAGPIE_TEST_READ_MODEL_PAIR_ROUTES=1000000 \
MAGPIE_TEST_READ_MODEL_EVIDENCE_DIR=/absolute/evidence/directory \
go test ./internal/database -run '^TestReadModelPairCostPopulatedPostgres$' -count=1 -timeout 35m -v
```

The final manifest records focused normal/race commands, exit codes, source and
evidence hashes. Combined full Go tests, race tests, vet and build must run again
after all backend changes, followed by the actual recovery and soak. This report
does not declare those later gates passed.
