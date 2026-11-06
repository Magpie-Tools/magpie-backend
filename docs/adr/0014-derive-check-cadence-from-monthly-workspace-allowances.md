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
- One **check** is one protocol test of one route for one workspace. Retries are
  part of that check and are still metered separately as `check_attempts`. One
  **scrape** is one fetch of one subscribed source for one workspace.
- Usage is metered in `workspace_usage_periods.checks` and `.scrapes` for each
  UTC calendar month, the same periods as the existing usage meter.

By default, cadence is derived rather than configured. Every minute, each instance loads
allowances and current-month usage and publishes an immutable snapshot
(`internal/checkpace`):

```
interval = time left in month × checks per cycle ÷ allowance left
checks per cycle = active managed routes × enabled protocols (tag rules included)
```

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
them once. The new instance default is the larger of the shipped default
(1,500,000 checks, 3,000 scrapes) and what the busiest workspace needed for its
old cadence over a 31-day month. No existing workspace becomes slower.

## Hot-path operation count

Per proxy check, with plaintext queue payloads (the default):

| Category | Before | After |
|---|---|---|
| Cryptographic operations | 0 | 0 |
| JSON encodes | 0 | 0 |
| Redis commands (pop, complete) | 3 (pop, `GET` interval, complete) | **2** |
| Database queries | unchanged | unchanged (the `checks` counter joins the existing usage upsert in the statistics batch) |

Outside the proxy check loop, each source fetch adds one small usage upsert
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

## Consequences

- An admin single-route requeue and newly imported routes follow the
  workspace's cadence. On a tight allowance, the first check of a new route can
  wait up to the workspace's interval instead of running at once.
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
