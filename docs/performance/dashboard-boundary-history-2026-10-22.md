# Restrict dashboard history to contributing boundary rows

The dashboard counter query combines complete daily counters with raw history
for the partial cutoff day and the current day. Its raw-history query previously
read every row since the weekly cutoff, although rows between those two day
boundaries contributed zero to both sums.

The query now adds:

```sql
AND (ps.created_at < next_day_start OR ps.created_at >= today_start)
```

The original `ps.created_at >= cutoff` condition remains. This preserves the
inclusive cutoff, exclusive next-day boundary and inclusive current-day start.
Complete middle days still come from daily counters. Compacted lifetime totals,
current ownership attribution, paused and archived ownership, shared routes and
the existing handling of future timestamps are unchanged. The helper still
executes two queries and uses the supplied context. There is no timer, worker,
cache, deadline, resource-profile or checker-path change.

## Scheduled read pressure

The captured driver-e log spans 00:41:54–05:05:11 UTC on 22 October. Its dashboard
prewarm took 130.914 seconds. Prewarm runs before the statistics consumers and
read-model routine start, although asynchronous queue bootstrap and rotating
listener synchronization have already started; it is not an isolated timing.

`StartDashboardCacheRoutine` uses a 30-second ticker. Each pass enumerates every
workspace and unconditionally refreshes info, eight recent checks and the
fastest 100 proxies. Workspaces run serially; the info loader starts six
concurrent branches. A full pass over eight non-dirty populated workspaces
executes at least 129 synchronous SQL reads:

| Component | Queries per workspace | Work per pass over eight workspaces |
| --- | ---: | --- |
| Info data | 8 | Daily totals, raw boundary history, two snapshot reads, country counts, judge counts, reputation counts and top reputation |
| Info generation checks | 2 | Before and after publication |
| Recent checks | 3 | One candidate query and two generation checks |
| Fastest proxies | 3 | One winner query and two generation checks |
| Workspace enumeration | Once per pass | One additional query |

The info branches include eight judge aggregates, eight raw-history aggregates
and sixteen country/reputation scans of the projection across the full pass.
The top-reputation query may also examine the workspace inventory, depending on
its plan. Successful daily reads schedule a separate two-second-delayed backfill
attempt per workspace. Its advisory-lock and missing-marker reads, and any
backfill writes, are outside the 129-query count. The in-flight map prevents
overlapping attempts for the same workspace, but completed no-op attempts can
be scheduled again on a later dashboard pass.

When a dashboard pass exceeds 30 seconds, the ticker's pending event starts the
next pass immediately. It does not create overlapping passes, but it provides
no idle interval between long passes. Refresh has lifecycle cancellation and no
separate per-pass timeout. Each process maintains its own dashboard cache and
refresh routine. Adding leader election alone would not preserve cache freshness
on other processes without distributing the resulting snapshots.

The log contains eight HTTP request entries, all during preflight, and no
dashboard refresh warnings. Successful pass durations are logged at debug level
and were not captured at the fixture's info level, so the number and durations
of background passes cannot be recovered. The final activity snapshot contains
a fastest query active for about eight seconds, alongside an I/O-waiting
projection refresh. Its three same-age PostgreSQL entries may be a leader and
parallel workers; the capture lacks `backend_type` and `leader_pid`, so they
must not be counted as three independent application queries.

Snapshots are a smaller recurring source of reads. The leader runs one metric
snapshot at startup and every ten minutes. The 26 completed snapshots had a
58.254-second median, a 45.106-second minimum and a 486.506-second maximum. Their
combined job wall time was 2,387.575 seconds; this is not a database-utilization
measurement. After 02:00, the median was 52.998 seconds. Hourly ownership-history
snapshots completed five times, taking about two to five seconds each. Projection
refresh logged 521 deadline failures, including continued failures after the
last statistics insert timeout at 01:36. Dashboard reads are therefore credible
competing work, but this evidence does not identify them as the sole cause of
the projection failure.

The other info branches do not offer the same independent zero-contribution
filter. Country and reputation counts count every selected row. Snapshot reads
already select one row. Top reputation already excludes NULL scores. Judge rows
with an unrecognized anonymity name add zero to the three sums, but can still
create an all-zero judge bucket; filtering them would change the returned
groups. No such broader change is included. A checker-generation-only refresh
skip would also be incorrect: physical results, ownership and reputation can
change without changing that generation.

## Quiet actual-fixture comparison

After the projection investigator explicitly released the target, the backend
remained stopped. Four statements ran sequentially in read-only transactions:
old/new result comparisons, followed by old/new analyzed plans. Each used a
15-second statement deadline and a two-second lock deadline, then rolled back.
There were no logical writes, vacuum, analyze, planner tuning or resource changes.
The preceding authorized rolled-back projection experiments had warmed and
modified physical buffers and generated WAL/dead versions. Normal autovacuum
could continue, and small source tests used a separate PostgreSQL instance.

The SQL was captured from the production helper on PostgreSQL. The old query
removes only the new predicate. Both queries returned `partial_cutoff_day=0`
and `current_day=999999` for workspace 2.

| Measure | Previous query | Boundary-filtered query |
| --- | ---: | ---: |
| Separate SELECT wall time | 3.320 s | 0.921 s |
| Analyzed execution | 1.050 s | 1.078 s |
| History rows visited | About 2,191,326 | About 2,191,326 |
| History rows admitted to the join | About 2,191,326 | 999,999 |
| Ownership rows visited | About 8,000,010 | About 8,000,010 |
| Temporary blocks read | 12,648 | 7,411 |
| Temporary blocks written | 12,732 | 7,480 |

The additional predicate removes roughly 1.19 million zero-contribution rows
before the ownership join and reduces temporary I/O. The existing history index
still visits those entries and filters them; the ownership plan still scans the
full table. Parallel node counts are per-loop averages and can differ from exact
row counts by a few rows after multiplication. The faster separate SELECT is
confounded by cache/order effects, and the analyzed runs show no latency gain.
This is a logical-work reduction, not proof that dashboard refresh now fits its
interval or that the running service has sufficient capacity. The separate
projection correction and combined application rehearsal remain necessary.

## Regression and evidence

The regression covers cutoff minus one microsecond, the cutoff itself, the end
of that day, the next-day boundary, middle days, the end of yesterday, today's
boundary, current and future rows, compacted totals, UTC and offset timestamps,
all ownership lifecycle states, shared routes, moved routes and unowned routes.
It compares the production result with expected totals and the prior boundary
sums, asserts two production queries, and checks canceled contexts.

Focused PostgreSQL normal/race checks passed in 2.865 and 6.059 seconds. The
SQLite check passed in 0.853 seconds. The recorded source hashes stayed unchanged
through each command.

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32772 user=magpie_boundary_test password=magpie_boundary_test dbname=magpie_boundary_test sslmode=disable' \
  go test -race ./internal/database \
  -run '^TestDashboard(DailyBoundaryCountsPreserveOwnershipAndCutoffs|CheckCountsFollowCurrentProxyOwnership)$' -count=1 -v
```

Accepted evidence is under
`.codex-tmp/release-remediation-20261020/dashboard-boundary-20261022/` in the
workspace: `focused-checks.json`, `generated/`, `actual-quiet/result.json` and the
complete plans, plus `read-pressure-analysis.json` derived from the retained log.
The coordinated driver owns final full Go checks and application validation.

This adds zero cryptographic operations, route hashes, JSON encodes, Redis
commands or database queries per proxy check. The dashboard uses the same two
database queries, with two additional bound timestamps in the history predicate.
