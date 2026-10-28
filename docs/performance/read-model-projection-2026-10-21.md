# Bound workspace proxy projection to its candidate evidence

The release rehearsal found a proxy filter projection still running after more
than nine hours on a million-route fixture shared by eight workspaces. The
original backend later exited with `drain read models: context deadline
exceeded`. The retained incident log is
`.codex-tmp/release-remediation-20261020/local-baseline/backend-before-recovery.log`
in the workspace.

The projection expanded the global current-evidence query independently for its
latest-result and health aggregates. The small-batch PostgreSQL plan fetched
latest pointers by `proxy_id` without restricting `workspace_id` in that index
lookup. With eight owners, each requested ownership pair could read the other
owners' pointers twice. A larger batch could also choose a different join plan.

The unscoped `WHERE up.proxy_id IN (...)` form runs in the background refresh
worker. Imports use the same projection with a workspace predicate while holding
their workspace subscription locks. The projection itself does not access
`workspace_subscriptions`. Its writes can contend with imports on the filter
rows, and a slow query consumes database resources while those transactions
wait. The source establishes those paths; it does not establish the exact lock
chain in the original incident.

## Change

The projection now materializes its requested workspace/proxy pairs, performs a
parameterized latest-statistic lookup for each pair, and materializes the matched
evidence once. Both aggregates reuse that bounded evidence. The existing
`idx_latest_proxy_workspace_protocol` index now receives both `proxy_id` and
`workspace_id` as index conditions. `OFFSET 0` keeps the lateral lookup from being
flattened back into the global ownership join. No index or schema change is
required.

The final upsert orders rows by workspace and proxy ID. Imports already sort
workspace IDs, but their per-workspace route inputs come from maps. Explicit SQL
ordering gives overlapping background and import projections the same filter-row
lock order.

The local query preserves generation-zero evidence, current sparse overrides and
default keys, and the existing transport behavior. It indexes committed keys
while a workspace is dirty; public readers still mask that workspace's pending
health. The shared `currentLatestStatisticsSQL` used by other readers is unchanged.

Background refresh batches have a configurable context deadline. A failed or
timed-out batch returns its routes to the existing dirty set. Shutdown still joins
the source worker, drains proxy invalidations, and then drains source counters.
Each drain batch receives the same deadline as an ordinary background batch.
Source-counter batches retain their existing thirty-second deadline. Final
draining retries requeued proxy or source batches with a 250-millisecond backoff
and does not report completion while either dirty queue contains work. This
corrects an initial timeout implementation that could return after its first
requeued failure. The outer runtime shutdown deadline reports an error if the
worker cannot finish draining. Cancellation detected during a schema lookup
also retains its proxy batch rather than treating a false table-existence result
as successful completion.

| Setting | Default |
| --- | ---: |
| `READ_MODEL_REFRESH_INTERVAL_SECONDS` | 5 |
| `READ_MODEL_REFRESH_BATCH_SIZE` | 5,000 routes |
| `READ_MODEL_REFRESH_MAX_BATCHES` | 4 per tick |
| `READ_MODEL_REFRESH_TIMEOUT_SECONDS` | 30 per batch |

Batch size counts distinct global routes. Five thousand routes shared by eight
workspaces can require forty thousand projection writes. Reduce the batch size
when workspace sharing or storage latency makes a batch exceed its budget.
The existing shutdown deadline still governs how long `Runtime.Shutdown` waits
for the complete drain. Dirty notifications remain in memory; the timeout does
not introduce durable retry storage.

## Measured regression

`TestReadModelProjectionScopesPopulatedEvidencePostgres` loads current and
obsolete latest pointers, eight shared owners, unrelated inventory, a sparse
QUIC override, a dirty workspace, a generation-zero owner, and a newly paused
route with no evidence. Candidate IDs are spread across the inventory and passed
in reverse order. Assertions cover projected values, unchanged unrelated rows,
public dirty-workspace masking, a single evidence build, and latest rows visited
in the actual PostgreSQL plan.

The large run used an isolated PostgreSQL 17 Alpine container with four CPU slots,
a 5 GiB memory limit, 512 MiB shared buffers, and 16 MiB work memory. It contained
1,000,001 proxy records, eight million shared ownership rows plus two special-case
ownerships, and more than four million latest pointers. The synthetic bulk load
followed restore ordering: load rows, restore and validate all four original
foreign keys, then measure. It did not disable foreign keys during measurement.

| Operation | Result |
| --- | ---: |
| Previous projection, 5,000 requested routes | 5.434 s |
| Scoped projection, same requested routes | 3.865 s |
| Scoped `EXPLAIN ANALYZE` execution | 4.978 s |
| Scoped ownership pairs | 39,994 |
| Latest rows visited, including obsolete keys | 79,988 |

The previous query completed in this synthetic fixture. It did not reproduce the
original nine-hour stall. These results establish bounded evidence access for
the tested sharing and batch size, not production capacity or a completed soak.
The two latency samples include database writes; their difference is not a
measurement of evidence lookup alone. The separate plan run incurs PostgreSQL
instrumentation overhead and repeats the upsert.

`TestReadModelProjectionTimeoutRetainsBatchPostgres` holds a real filter-row lock,
runs a background batch with a one-second budget, and verifies the dirty route
survives cancellation, including a canceled schema lookup.
`TestReadModelShutdownRetriesTimedOutProjectionPostgres` keeps that lock across
two one-second deadlines during final shutdown and verifies the worker remains
active until the row unlocks, then drains both queues. The source-failure
regression similarly injects repeated errors, checks backoff, and verifies
recovery. Existing bootstrap deadline coverage verifies an unfinished read-model
worker makes runtime shutdown return `context deadline exceeded`. Existing
source shutdown and checker-attribution tests also pass.

Reproduce against disposable services:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_read_model_test password=magpie_read_model_test dbname=magpie_read_model_test sslmode=disable' \
  go test ./internal/database -run 'TestReadModelProjection|TestReadModelShutdown|TestSourceStatsShutdown|TestCanceledSourceStats' -count=1 -v

MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_read_model_test password=magpie_read_model_test dbname=magpie_read_model_test sslmode=disable' \
MAGPIE_TEST_READ_MODEL_ROUTES=1000000 MAGPIE_TEST_READ_MODEL_BATCH=4999 \
  go test ./internal/database -run '^TestReadModelProjectionScopesPopulatedEvidencePostgres$' -count=1 -v -timeout=20m
```

The large run adds one new route to the configured 4,999 existing candidates.
`MAGPIE_TEST_READ_MODEL_EVIDENCE_DIR` optionally saves the analyzed plan and
latency JSON. `MAGPIE_TEST_READ_MODEL_BASELINE_SQL` optionally runs the retained
previous SQL with a fifteen-second deadline and saves its planned query and
latency. The baseline SQL, plans, logs, and required-check runner are retained in
`.codex-tmp/release-remediation-20261020/read-model/` in the workspace.

## Checker operation budget and validation

The dequeue/check/requeue path and asynchronous statistics transaction are
unchanged. Added operations per proxy check are zero cryptographic operations,
zero JSON encodes, zero Redis commands, and zero database statements. A projection
chunk still uses its existing single upsert statement and table-existence probe.
Materialization, the bounded lookup, and sorting run inside that statement. The
new context timer runs once per background batch. Queue credential encryption
remains opt-in, and normal requeues still update scheduling only.

Required validation uses Go 1.27.2, `GOMAXPROCS=4`, and the separate PostgreSQL and
Redis 7 fixtures. It includes `go test ./... -count=1`,
`go test -race ./... -count=1`, `go vet ./...`, and `go build ./cmd/magpie`.
The race run supplies a race-built backend binary to subprocess integration
tests. Exact command results and durations are recorded in
`read-model/required-checks.json` alongside the logs.

The first combined run completed successfully on 21 October, before the final
shutdown retry and later API/index corrections:

| Check | Elapsed |
| --- | ---: |
| Full normal tests | 338.355 s |
| Full race tests | 382.146 s |
| Vet | 3.308 s |
| Build | 1.248 s |

The raced subprocess binary also built successfully in 15.233 seconds. That run is preserved under
`read-model/required-checks-before-api-followups/`; the combined checks must run
again after those follow-up changes are final. The final drain regressions pass
in `read-model/shutdown-retry-targeted.log`: canceled schema lookup retention,
background timeout retention, a row locked across two final-drain deadlines,
source failures with backoff and recovery, and final source invalidation drain.
The final validation driver records the combined full-check run under
`.codex-tmp/release-remediation-20261020/backend-final-checks/`.
