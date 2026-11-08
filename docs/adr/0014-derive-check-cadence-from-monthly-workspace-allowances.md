---
status: accepted
---

# Derive check cadence from monthly workspace allowances

Administrators used to set one checker timer and one scraper timer for the
whole installation. Every route was rechecked at that interval, whatever its
workspace paid for, and the leader stored the interval in Redis
(`magpie:queue:proxy:interval_ms`, `magpie:queue:scrape:interval_ms`). Each
normal requeue read that key, which cost one extra Redis command per check.

Hosted plans sell a monthly number of checks per workspace. Cost grows with
routes × protocols × checks per day, so cadence has to follow the allowance
rather than a global timer.

## Decision

Each workspace has a monthly check allowance and a monthly scrape allowance:

- `workspace_subscriptions.monthly_checks` and `monthly_scrapes` set them. Zero
  inherits the instance defaults `checker.monthly_checks_per_workspace` and
  `scraper.monthly_scrapes_per_workspace`, which replace `checker_timer` and
  `scraper_timer`.
- One **check** is one outbound request to a judge for one workspace. Retries
  are checks, so a protocol test with two retries uses three. Workspace
  settings accept up to 255 retries; charging per request means retries and
  the worker time they occupy cannot multiply work without consuming
  allowance. One **scrape** is one fetch of one subscribed source for one
  workspace.
- Usage is metered in `workspace_usage_periods.checks` and `.scrapes` for each
  UTC calendar month, the same periods as the existing usage meter.
  `protocol_checks` counts protocol tests and is used only to estimate
  requests per test.

### Metering independent of statistics

The statistics pipeline may drop or delay results under overload, so it
cannot be the source of allowance usage. The checker charges each request
just before sending it, after the lease renewal for that attempt
(`checkpace.Snapshot.ChargeRequest`). The charge first compares the
snapshot's usage plus this instance's unseen checks with the allowance and
refuses the request when nothing is left. Otherwise it adds to an in-memory
per-workspace, per-month counter. A refused workspace leaves the assignment:
it receives no further requests, retries or result for that protocol test,
and an unfinished test does not count toward its failure streak. Workspaces
sharing the request keep it. When no workspace is left, the request is not
sent.

The limit is soft. Admission and charge are not one atomic step, and other
instances' usage arrives with the next refresh, so a workspace can go past
its allowance by at most one request per worker serving it on this instance,
plus about one minute of other instances' checks. Magpie absorbs this
overshoot rather than coordinating instances on every request.

The pacer flushes the counters every 10 seconds and immediately before each
snapshot load, under one mutex, and records how much of each counter the
database holds. A snapshot therefore knows exactly which local checks it has
not yet seen. Counters are kept per UTC month, so usage is written to the
month in which the request was made, however late it is flushed. Each flush
is a numbered batch from a process identity chosen at startup. A failed batch
is retried unchanged before new usage is batched, because its commit may have
succeeded without an acknowledgement. The database records each process's
last applied sequence in `workspace_usage_flushes` within the same
transaction and skips any batch at or below it, so a replay never charges
twice. Marks of processes that stopped flushing more than seven days ago are
deleted at startup. While a batch is unacknowledged, the pacer keeps
its current snapshot instead of loading usage, because a load could count the
committed batch both in the database and in the local counters. The kept
snapshot still counts every local request, and the flush that finally
commits the batch reloads at once. `check_attempts`, written by the statistics pipeline,
remains an analytics counter and is not used for allowances.

By default, cadence is derived rather than configured. Every minute, each instance loads
allowances and current-month usage and publishes an immutable snapshot
(`internal/checkpace`):

```
interval = time left in month × checks per cycle ÷ allowance left
checks per cycle = protocol tests per cycle × requests per test
protocol tests per cycle = active routes × default protocols
                         + Σ over active routes with tag-rule plans (plan protocols − default protocols)
requests per test = this month's checks ÷ protocol_checks (1 until 100 tests are seen)
```

Tag-rule plans exist in the checker snapshot for paused and archived routes
too, which are not checked. Their contribution therefore needs a count of
which of those routes are active. Plans that enable as many protocols as the
default need no count. The result is cached per workspace until the checker
snapshot or the active route count changes, a lifecycle write on this
instance invalidates it, or ten minutes pass.

The interval is limited by `max(1 minute, subscription minimum check interval)`
for checks and by 15 minutes for scrapes. Because the snapshot is recomputed
from remaining allowance, estimation errors correct themselves: a workspace
that used checks faster than planned slows down, and its allowance cannot run
out before the period ends. A workspace with no allowance left is not checked
until the next period.

### Fixed intervals chosen by the workspace

Operators may instead choose a fixed check interval and scrape interval
(`workspaces.check_interval_seconds` and `scrape_interval_seconds`, set with
`PUT /api/workspace/cadence`; zero means automatic). A fixed interval is used
as is, subject to the same minimums, and does not follow the remaining
allowance. Checks or scrapes stop as soon as the allowance is used up and
resume next period. Intervals below the minimum or above 31 days are rejected.

The snapshot projects usage at the current cadence: per-cycle work × period
length ÷ interval, and for a fixed interval the moment the remaining allowance
runs out. The API and the Workspace page warn when a fixed interval needs more
than the allowance. The page warns live while the interval is edited, and the
save response repeats the warning. The projection uses the latest per-cycle
count, so it is approximate. The checker hot path is unchanged: a fixed
interval is just another value in the in-memory snapshot.

### Unlimited allowances

An administrator may remove the limit for one workspace
(`workspace_subscriptions.unlimited_checks` and `unlimited_scrapes`) or as the
instance default (`checker.unlimited_monthly_checks` and
`scraper.unlimited_monthly_scrapes`). An explicit monthly allowance on the
subscription overrides an unlimited default. Automatic spreading has nothing to
divide, so an unlimited workspace must choose a fixed interval. The cadence API
rejects `0` for it. Until the workspace chooses an interval, it runs every hour
(`checkpace.UnlimitedDefaultInterval`), which keeps an instance-wide switch to
unlimited from silently stopping or flooding existing workspaces. Unlimited
workspaces are never exhausted, report no run-out and receive no
over-allowance warning. Usage is still metered. The plan's minimum interval
still applies.

### Shared routes and slow cadences

A route is requeued after its fastest owner's interval, capped at six hours.
On each dequeue, the checker includes an owner only when one of that owner's
own cadence boundaries falls within the route's last cycle window. Each route
gets its own phase, derived from the route and workspace IDs. Slower owners on
a shared route, and owners slower than the six-hour cap, are therefore checked
about once per their own interval without per-workspace state. If no owner is
due, the cycle requeues immediately: no outbound request, no failure tracking
and no ownership query. The scraper applies the same rule per fetch mode.

The six-hour cap bounds how long a refilled or upgraded allowance takes to
apply, and bounds the cost of idle revisits to one dequeue per route every six
hours.

### Upgrades

If a settings file still has the old timers and no allowances, startup converts
them once without changing behavior. The old timers had no monthly limit, so
the instance default becomes unlimited, and every existing workspace gets the
old timer as its fixed interval (raised to the minimum and capped at 31 days).
An earlier draft gave automatic spreading a large default allowance, which
made a one-route workspace on a five-hour timer check every minute. Workspaces
created after the upgrade run hourly until they choose an interval.

## Hot-path operation count

Per proxy check, with plaintext queue payloads (the default):

| Category | Before | After |
|---|---|---|
| Cryptographic operations | 0 | 0 |
| JSON encodes | 0 | 0 |
| Redis commands (pop, complete) | 3 (pop, `GET` interval, complete) | **2** |
| Database queries | unchanged | unchanged (check usage is flushed in batches every 10 seconds per instance, not per check) |

Each request adds, per workspace it serves, a snapshot map read, an atomic
load and one atomic addition (two on a test's first request), and each
owner's due decision reads one atomic counter. A proxy cycle allocates one
small meter, and an assignment copies its workspace list only when a
workspace runs out during it.

Outside the proxy check loop, each instance writes one batch per 10 seconds
for the workspaces it checked: one transaction with a batch mark upsert and
one usage upsert row per workspace and month. Each source fetch adds one small usage upsert
for the workspaces it served. Scrapes run at most once per subscribed source
every 15 minutes. Each instance also runs one set of aggregate allowance
queries per minute.

The added work is in-memory only. It consists of map lookups in the snapshot
and integer arithmetic over a route's owners, with no allocation for
single-owner routes. `TestPlaintextProxyCycleCommandBudgetAndPayload` now
asserts the two-command budget. The tests proving that a current plaintext
payload reuses its stored hash, and that a normal requeue does not rewrite the
payload, are unchanged.

## Measurements

The measurements used a disposable `redis:7-alpine` container on the same host,
an i7-12700H, Go 1.27.3, and the existing benchmark:
`go test ./internal/jobs/queue/proxy -bench BenchmarkProxyQueueCycle
-benchtime=20000x -count=5`.

| Benchmark | Before median | After median | Allocations per operation |
|---|---|---|---|
| `BenchmarkProxyQueueCycle` | 164.8 µs/op | 123.8 µs/op (−25%) | 53 → 47 |
| `BenchmarkProxyQueueCycleConcurrent` | 52.5 µs/op | 40.7 µs/op (−22%) | 54 → 48 |

`BenchmarkSelectDueWorkspaces`, the new per-dequeue decision, takes 46 ns with
0 allocations for one owner. For a route shared by eight owners on different
cadences, it takes 0.87 µs with one 4.8 KB allocation (the filtered owner list).
These figures include reading each owner's metered usage. Snapshots store
paces by pointer, which kept the meter from adding a per-owner struct copy. In
back-to-back runs on the same host it measured 46 ns and 0.86 µs, against
90 ns and 1.3 µs for the version without the meter. `BenchmarkChargeRequest`
takes 13 ns with 0 allocations. `BenchmarkRequestMeterAdmit`, the whole
admission before a request including the clock read, takes about 70 ns for
one owner and 0.22 µs for eight, with 0 allocations. A judge request takes
milliseconds.

## Consequences

- An admin single-route requeue and newly imported routes follow the
  workspace's cadence. On a tight allowance, the first check of a new route can
  wait up to the workspace's interval instead of running at once.
- A checker stops sending a workspace's requests, retries included, as soon
  as its own count reaches the allowance. Requests admitted concurrently can
  overshoot by one per worker. Other instances' checks are seen at the next
  snapshot refresh, so a workspace checked by several instances can overshoot
  by about one minute of their checks. Requests are charged before they are
  sent, so a crashed instance loses at most its last 10 seconds of charges.
- A workspace that runs out during a protocol test's retries has paid for the
  requests already sent and receives no result for that test.
- On a shared route, the gap between a cycle's due time and its completion is
  not covered by any window. Slow owners can therefore receive slightly fewer
  checks than their cadence allows. The recomputed snapshot compensates by
  shortening their interval.
- Allowances follow UTC calendar months, not billing periods
  (`current_period_start` and `current_period_end`). Aligning them with
  billing is future work.
- Overage beyond the check allowance is not implemented. An exhausted workspace,
  whether its interval is automatic or fixed, simply stops being checked until
  the next month.
- ADR 0002 is unaffected. The queue payload format and plaintext default have
  not changed.
