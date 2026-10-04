# Tag checker settings operation budget

Measurements taken on 2026-10-03 use Linux amd64, an Intel Core i7-12700H,
Go 1.27.1, and local PostgreSQL 17. They isolate configuration lookup and
statistics persistence. They do not estimate network-check throughput or
replace the distribution's load and soak gate.

## Shared profile revision

Default and tag rules now share transport, timeout, and retries across enabled
protocols. This changes snapshot resolution and API payloads only. Relative to
the original tag implementation, each check adds 0 cryptographic operations,
0 JSON encodes, 0 Redis commands, and 0 database queries. Workers read the same
compiled plans; queue credential policy, payload decoding, requeue writes, and
result persistence are unchanged. Reachable transport validation and reading
earlier draft profile JSON run on settings reads or writes outside the hot path.

## Projection and cache correction

Measurements on 2026-10-04 use the same local PostgreSQL 17 setup. In a workspace
with 10,001 routes, adding an unrelated classification tag and explicitly
reconciling took 14.33 ms and rewrote zero filter rows. Adding or removing a
checker tag rewrote exactly one filter row. A PostgreSQL update trigger counts
the writes. A stable projection refresh also writes no filter rows.

With 1,000,000 unrelated latest results and one source candidate with current
alive evidence, `EXPLAIN (ANALYZE, BUFFERS)` measured 0.218 ms execution for the
source-detail health aggregate, with 1.081 ms planning. Its latest-statistics access uses
the proxy/workspace/protocol index and aggregates one candidate's evidence. This
measures the health aggregate, excluding the detail endpoint's other queries and
application latency.

The initial correction scoped projection writes, but still loaded every checker
assignment and compared the full sparse projection. Independent measurements with
a million tagged routes exposed 8.9 seconds and 1.39 GB allocated for a one-route
edit, and 8.3 seconds for unchanged reconciliation. A dashboard query that
correlated health before limiting also performed a million evidence aggregates.

The revised loader compares cached workspace revisions and small configuration
signatures before reading assignments. A coalesced per-route revision journal
selects affected routes, including edits projected by another instance and missed
changes followed by reversions. Incremental refresh reads only those routes'
checker assignments and persisted overrides. Immutable map shards copy the
affected part of the snapshot while preserving plans held by running checks.
Column-only saves bypass checker reconciliation and preserve workspace revisions.
Startup and changes to Default, judges, headers, or SOCKS judge routing still
require a complete snapshot. Editing a rule that matches many routes necessarily
processes all of those matches.

The updated `TestCheckerRefreshAndRecentChecksScalePostgres` fixture gives every
one of 1,000,000 routes a checker-tag override. A second rule matches one route.
The test measures real production mutation and loader calls, not writes alone:

| Operation | Elapsed | Go bytes allocated |
| --- | --- | --- |
| Unchanged cached reconciliation | 12.50 ms | 144,928 |
| Add the second checker tag to one route | 54.02 ms | 539,104 |
| Change that route's checker rule | 39.51 ms | 431,600 |
| Delete one tagged membership and refresh its snapshot | 101.50 ms | 427,896 |

SQL tracing asserts that unchanged reconciliation reads no assignment, override,
or journal rows. Incremental reads are restricted to changed proxy IDs. Prior
snapshots and unrelated plan pointers remain unchanged.

Recent dashboard checks select ten candidates from the active health projection
using `idx_user_proxy_filter_recent_active` before fetching evidence. The actual
refresh measured 7.27 ms end to end. Its `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`
execution took 0.579 ms; each latest-evidence access executes at most ten times.
The materialized limit prevents evidence aggregation across the whole workspace.

These changes add zero cryptographic operations, JSON encodes, Redis commands,
or database queries per proxy check, including asynchronous result persistence.
Mutation-side work adds workspace revision writes and coalesced journal upserts,
outside checker workers. The loader uses three small JSON encodes for input
signatures per refresh; unchanged reconciliation still compiles the small Default
plan, but reads no route data. Rule identity hashes remain refresh-time work.
The journal stores one coalesced row per changed managed route and temporary
tombstones for removed memberships. Retention maintenance expires committed
tombstones after 24 hours using a partial index; it advances the full-refresh
watermark before removing them. A process that missed recovery information must
reload its snapshot. Workspace deletion cascades the journal. The migration adds
revision counters, journal indexes, and the dashboard candidate index.

Ownership deletion captures changed routes inside the existing DELETE statement,
before assignment and override cascades. Two indexed existence probes select
routes with cached overrides or prior journal entries; only matching routes
advance a workspace revision and upsert a tombstone. The default failure-action operation
budget remains 384 SQL statements for 32 transitions, equal for Pause and Delete.
An updated local sample measured 31.50 ms per Pause and 27.89 ms per Delete at
10,000 routes. A tagged removal also refreshes its affected snapshot and publishes
one workspace invalidation when Redis is connected. This terminal ownership
mutation work preserves independent membership settings and never runs on an
ordinary dequeue, check, or requeue. It performs no credential cryptography or
queue payload rewrite beyond the existing ownership-change path.

Dashboard cache reads verify the persisted generation with one API-side query.
Refreshes read it before and after loading results, rejecting in-flight obsolete
results. These queries support multiple backend instances and never run in
checker workers. Stable reconciliation does not invalidate dashboard entries.

Reproduce the regression cases and the larger source workload with:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=55437 user=checker_test password=checker_test dbname=checker_test sslmode=disable' \
  go test ./internal/database -run 'TestCheckerTagProjectionWrites|TestCheckerMissed|TestDashboardRefreshRejects|TestSmallSource' -count=1 -v

MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=55437 user=checker_test password=checker_test dbname=checker_test sslmode=disable' \
MAGPIE_TEST_CHECKER_SOURCE_ROUTES=1000000 \
  go test ./internal/database -run '^TestSmallSourceHealthAggregatesCandidatesPostgres$' -count=1 -v

MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=55437 user=checker_test password=checker_test dbname=checker_test sslmode=disable' \
MAGPIE_TEST_CHECKER_SCALE_ROUTES=1000000 \
  go test ./internal/database -run '^TestCheckerRefreshAndRecentChecksScalePostgres$' -count=1 -v -timeout=20m
```

## Operations per check

Tag resolution reads an immutable in-memory workspace snapshot. Configuration
keys are calculated when settings, judges, or relevant assignments refresh.
Queue workers reuse the stored route hash and ordinary requeues update only
the existing sorted-set scheduling state.

| Added operation | Rule lookup and queue loop | Asynchronous result persistence |
| --- | --- | --- |
| Cryptographic operations, nonce generation, or route hashes | 0 | 0 |
| JSON encodes | 0 | 1 per persisted physical event for the history evidence column |
| Redis commands | 0 | 0 |
| Database statements | 0 | Depends on workspace fanout and batch size, measured below |

For a proxy cycle that persists P distinct judge/protocol/transport/budget
assignments, the additional history-column encode count is P. Matching tags
alone do not add encodes. The SQL budget below is amortized across batches of
physical events, rather than one transaction per proxy cycle.

The existing statistics-stream encoding still happens once per physical
event. Its payload includes participating workspace verdicts and configuration
keys, so it becomes larger without adding another stream encode. GORM encodes
that evidence for the PostgreSQL JSONB column during batched persistence. This
additional encode preserves workspace validation attribution in history and
allows current configuration and uptime queries to reject incompatible results.
One physical response, history row, and route reputation event remain shared.

Attribution writes a latest pointer for each participating workspace. It avoids
redundant global latest and overall writes for new attributed events. Legacy
unattributed writers retain those writes for compatibility. Extra latest rows
are required because a shared physical response can pass one workspace's judge
validation and fail another's. Batches cap latest inserts at 3000 rows to stay
within PostgreSQL's extended-protocol parameter limit.

Settings refresh, SQL projection, pending-state health masking, Redis publication,
and retention pruning run outside the checker loop. Health APIs may read the
workspace's pending flag before serving cached results. None of these reads
is a per-check credential or tag lookup.

After the shared-profile revision, `BenchmarkSnapshotLookup` measured 16.98 ns
per lookup, 0 allocated bytes, and 0 allocations. Workers still use the same
snapshot lookup and compiled plan.

## Configuration lookup and memory

`BenchmarkSnapshotLookup` measured 16.61 ns per lookup with 0 allocated bytes
and 0 allocations. It reads the cached workspace and route plan, with no loader
call. These initial measurements used a flat route map. Tests explicitly verify
that lookup never invokes the database loader.

After introducing immutable shards, three runs of `BenchmarkSnapshotLookup`
measured 22.55–28.87 ns with no allocations. `BenchmarkTaggedSnapshotLookup`
reads a million-route map and measured 95.43–106.2 ns with no allocations. These
runs overlapped PostgreSQL fixture work and are isolated lookup measurements,
not network throughput estimates.

`BenchmarkBuildTaggedSnapshot` uses one million tag assignments selecting one
shared override plan. Three runs after retained-memory instrumentation measured
362.88, 376.04, and 380.74 ms per build. Builds allocated about 75.5 MB in total
and 8224 to 8230 objects per build. The retained snapshot added 37.70 to 37.78 MB
after garbage collection, excluding the already-allocated assignment input.
These values describe this shared-plan workload. More distinct combinations
need additional plans; tags that do not change effective settings produce no
route override entry.

An updated one-build sample using immutable shards took 283.26 ms and allocated
78.07 MB, retaining 39.26 MB after collection. Shards trade a small memory increase
and more map objects at initial compilation for sub-megabyte one-route edits.

The existing statistics-stream marshal benchmark isolates payload expansion:

| Participating workspaces | Encode time | Allocated bytes | Allocations |
| --- | --- | --- | --- |
| 0, unattributed comparison | 2291 ns | 1793 | 3 |
| 1 | 2518 ns | 1922 | 3 |
| 8 | 3941 ns | 2691 | 3 |

## PostgreSQL persistence

`TestCheckerEvidencePersistenceOperationBudgetPostgres` processes 1000 distinct
physical events per batch at one and eight workspaces. Both variants use the
same schema, ownership, usage records, and pipeline. One warm-up batch precedes
three measured batches. Client-side SQL tracing excludes transaction boundaries
and includes savepoints. This comparison isolates attribution fanout within the
new schema; it is not a benchmark of an older binary.

| Workspaces | Attribution | SQL statements per batch | Mean batch time | Physical events per second |
| --- | --- | --- | --- | --- |
| 1 | Unattributed comparison | 7 | 96.32 ms | 10382 |
| 1 | Attributed | 6 | 66.76 ms | 14978 |
| 8 | Unattributed comparison | 7 | 74.54 ms | 13416 |
| 8 | Attributed | 9 | 220.64 ms | 4532 |

At one workspace, removing redundant overall persistence saves one statement.
At eight workspaces, parameter-safe latest inserts and their savepoint add two
statements net per 1000 physical events, or 0.002 statements per event amortized
for this workload. The test asserts both budgets. Row fanout and larger JSON
reduce throughput substantially at eight workspaces even though statement
fanout is bounded. Releases must size statistics workers and run the load gate
with representative workspace sharing; high sharing is a material persistence
cost of independent health evidence.

## Reproduce

From `magpie-backend`, use an isolated PostgreSQL test database:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=55437 user=checker_test password=checker_test dbname=checker_test sslmode=disable' \
  go test ./internal/database -run '^TestCheckerEvidencePersistenceOperationBudgetPostgres$' -count=1 -v

go test ./internal/checkerconfig -run '^$' -bench '^Benchmark' -benchmem -benchtime=1s -count=3
```

Run the required full normal and race tests, vet, and build before merging.
Current plaintext queue payloads must reuse their stored hash without invoking
the cipher. Normal requeues must not rewrite payloads. These regression tests
remain unchanged and pass with the tag checker implementation. Queue encryption
remains opt-in and `PROXY_QUEUE_ENCRYPT_CREDENTIALS=false` remains the default.
