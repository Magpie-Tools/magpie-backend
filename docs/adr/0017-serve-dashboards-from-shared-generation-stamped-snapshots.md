---
status: accepted
---

# Serve dashboards from shared, generation-stamped snapshots

The dashboard used an in-process cache in the core `database` package: three
`sync.Map` value caches, a coordinator map with demand tracking, flights and a
revision counter, a 30 s timer that recomputed every key read in the last two
minutes, a startup prewarm and a fixture prewarm. Each backend instance kept
its own copy, so N instances computed every viewed dashboard N times. Expired
entries were never served: a reader waited for a full recompute. A cold read
also started a delayed legacy backfill of per-proxy daily check counters under
a global advisory lock, and the read waited for it.

The design was agreed over five review rounds (Opus 5.5, GPT-6.1 Sol, Claude
Fable 5.2 and an Opus 5.5 reviewer). This record keeps the decision and the
owner's answers; the cost analysis is in the performance note written by the
measurement step.

## Decision

One PostgreSQL row per workspace and payload version in `dashboard_snapshots`
replaces the in-process cache. The coordinator, the value maps, demand
tracking, the revision counter, the timer, the prewarms and the read-path
backfill are deleted.

- The row is built on demand by the instance that receives a request. A claim
  takes a lease epoch from `dashboard_lease_epoch_seq`, so a recreated row
  never reuses an ownership token, and every publish or failure update is
  fenced by that epoch.
- Claims are admitted under one cluster-wide cap (`C`, default 4) checked
  under an advisory transaction lock with a 250 ms `lock_timeout`, plus a
  local semaphore (default 2) and a per-instance singleflight. A workspace is
  rebuilt at most once per 30 s cluster-wide, and only after a read.
- The payload has inventory parts, valid at any checker generation, and health
  parts, valid only for the generation that built them. A health part is never
  served after a newer committed generation, and while `checker_dirty` is true
  health is masked exactly as before.
- Every response has an explicit status, decided in this order:
  - `FRESH`: a payload younger than 30 s whose health is current or masked by
    a dirty workspace, including one a build published during the request.
  - After a failed build: `STALE` if any part is valid, `FAILED` if none is.
  - `STALE`: a payload up to 10 minutes old whose health is current or masked.
  - `PENDING` otherwise: no payload, a payload older than 10 minutes, or health
    missing after a generation change, while no build has published yet. The
    valid parts are still served.

  Age never hides a part; only the generation and the dirty flag hide health,
  and dirtiness alone never makes a view `PENDING` or triggers a rebuild. A
  stale or cold read waits at most 2 s for a build. Absent parts are null,
  never fabricated zeros.
- The code lives in `internal/dashboard`, a feature package above the core.
  The core never imports it and has no invalidation hook into it: the
  generation stamp replaces invalidation.

### Owner decisions (14 November 2026)

1. A 10-minute stale window, the pending and rebuilding UX, and a 60 s
   re-request window in the frontend.
2. REST `getDashboardInfo` and the GraphQL fields `dashboard`,
   `recentProxyChecks` and `fastestAliveProxies` have no users outside the UI.
   They are removed in the release in which the frontend moves to
   `viewer.dashboardView`, without a compatibility layer.
3. The legacy daily-counter backfill is deleted rather than finished.
4. a. "Total checks" counts only the workspace's own usage.
   b. "Fastest alive" ranks successful checks under the workspace's current
      checker settings, which is the existing query.
5. "Checks this week" becomes "checks this month", the allowance period.

### Check counts come from the usage meter

Both figures are read live on every view from `workspace_usage_periods`, in
the same statement that reads the snapshot, so they are never stale and are
never stored in the payload.

- The unit is outbound check requests, retries included: the number the
  monthly allowance charges ([ADR 0014](0014-derive-check-cadence-from-monthly-workspace-allowances.md)).
- `checksThisMonth` is `checks` for the current period
  (`checkpace.UsagePeriod`), the column behind the allowance. It is 0 when the
  period has no row yet; rows appear on the first write of a period. The
  workspace page adds this instance's unflushed usage to its per-minute pacing
  snapshot, so it can run up to about a minute ahead of the dashboard.
- `totalChecks` is `SUM(GREATEST(checks, check_attempts))` over every period.
  `check_attempts` is written by statistics ingestion and is lossy under
  load; `checks` has been metered by the checker only since 8 November 2026.
  Adding the two columns would count the same requests twice. Lifetime totals
  start when the workspace started, so existing installations show a lower
  total than before, which counted checks of the workspace's routes by any
  workspace.

### Per-proxy daily check counters are deleted

`proxy_daily_checks`, `proxy_check_totals` and
`proxy_daily_check_proxy_backfills` had no reader except the dashboard check
counts. Their writers go: the upsert in every statistics ingestion
transaction (a write that grows with check volume), compaction, orphan
cleanup, the read-path backfill, the development seed and
`PROXY_DAILY_CHECK_RETENTION_DAYS`. The remaining ingestion order is
unchanged: history, latest evidence, reputation refresh, workspace usage,
legacy aggregate.

The tables are dropped by a schema step one release after the release that
stops writing them, so adjacent releases in a rolling upgrade never write to a
dropped table. Multi-instance deployments must not skip that release: older
ingestion would fail its whole transaction on a missing table. Upgrades that
skip releases keep the procedure of stopping every backend first.

### Fastest alive semantics are fixed

Any later rewrite, including the deferred merge of the alive-evidence scans,
keeps: latest evidence per proxy, protocol and current config key, honoring
sparse plan overrides; the generation-0 path and dirty masking; actively owned
routes only; successful checks only; the best response time per proxy; and the
existing tie-breakers, which also give payload slicing a total order. The read
model's latest response time is not an acceptable substitute.

## Consequences

- A warm view is one joined read (snapshot row plus two primary-key range
  reads of usage) besides the unchanged authentication and the bounded
  history and series reads. A workspace with no viewers costs nothing apart
  from the retention sweep.
- A build runs about seven existing data statements, at most two at once, each
  in its own read-only transaction with a local `statement_timeout`, under a
  30 s deadline that is independent of the HTTP write timeout.
- Builds are shared across instances; at most `C` admitted builds run in the
  cluster. Timeout-bounded SQL from an expired lease can overlap its
  successor, so the cap is exact for admitted leases only.
- A browser tab running the previous frontend after an upgrade gets a GraphQL
  validation error on the dashboard and shows the existing error state until
  it is reloaded. Only the dashboard sends that query.
- The hot path is unchanged: no cryptographic operation, JSON encode, Redis
  command, query or write is added per check, and one write per accepted
  statistics batch is removed.
- `C`, the per-build statement concurrency and the build deadline are set from
  measurements on matched hardware. The release that removes the old fields is
  blocked until ordinary million-route builds finish within the deadline.

## Rejected alternatives

- **Keep the in-process cache and fix its timer.** Every instance would still
  compute every viewed dashboard, and freshness would still depend on local
  demand bookkeeping.
- **A job table with pollers.** It adds a background routine and idle load for
  work that only a read needs.
- **Redis as the snapshot store.** PostgreSQL is the system of record, the
  build reads it anyway, and snapshot rows cascade with their workspace.
- **Blocking cold reads until a build finishes.** A million-route build can
  exceed the HTTP write timeout; an explicit `PENDING` status is honest.
- **Compatibility adapters for the old fields.** Only the bundled frontend
  calls them.
