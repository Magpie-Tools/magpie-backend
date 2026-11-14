---
status: accepted
---

# Schedule routes on workspace cadence grids

[ADR 0014](0014-derive-check-cadence-from-monthly-workspace-allowances.md)
requeued a route after its fastest owner's interval and counted every dequeue
as that owner's check. A slower owner was checked when one of its own cadence
boundaries fell inside the window of the route's last interval. That rule had
four defects, measured by replaying the queue against the scheduling code of
`57231ce` for many routes in simulated time:

| Defect | Measured at `57231ce` |
| --- | --- |
| An automatic cadence is recomputed at every refresh, and a slower owner's boundaries were its route hash modulo that cadence. A change of milliseconds moved every boundary, so the owner was sampled at random. This also affected a single owner slower than six hours, which is every automatic workspace above about 3,100 routes on the default allowance. | Automatic, about 1 hour, sharing with a 5-minute owner: median gap 40 minutes, 99th percentile 4.5 hours, longest 13 hours 50 minutes. Automatic, about 96 hours, alone: median 60 hours, 99th percentile 354 hours. The published cadence moved only between 59m57s and 1h0m1s. |
| A requeue was `completion + interval`. A longer interval kept the routes in the band of the old one, and when the fastest owner ran out, the next owner was due on every shared route at once. | 20,000 routes shared by a 5-minute and a 6-hour owner: after the 5-minute owner ran out, all 20,000 were checked for the 6-hour owner within 5 minutes, 72 times the even rate, and again in the same 5 minutes every 6 hours. The same happened to one workspace that changed 5 minutes to 6 hours. |
| The window covered the route's interval but not the time from the due time to the end of the check. A slower owner's boundaries in that time were in no window. | Fixed 1 hour, sharing with a 1-minute owner: 86% of its checks when a cycle took 10 seconds, 67% at 30 seconds, 39% at 90 seconds, 17% at 5 minutes. Automatic compensated; fixed did not. |
| An import left an already queued route at its due time. | A new owner with a 1-minute cadence waited up to six hours for the first check of a route that a slow workspace already owned. |

## Decision

### A cadence is a grid of points in time

Each workspace has, on each of its routes, fixed points in time at which it is
due (`checkpace`'s `grid`). A point depends only on the cadence and on the
route. Every instance computes the same points, nothing is stored per route or
per workspace, and a route's points do not move while the cadence stays the
same. The route hash spreads a workspace's routes across its cadence.

The grids of all owners of one route are laid over each other, because the
hash no longer includes the workspace. Owners on the same cadence have the
same points. The points of a fixed interval are points of every fixed interval
that divides it, and the points of an automatic cadence are points of every
faster automatic cadence with the same minimum interval. Owners that are due
at the same point share one check of the route.

A cadence that a workspace or its plan chose (a fixed interval, the hourly
default of an unlimited workspace, a minimum interval) is constant. Its grid
has one point per interval.

An automatic cadence changes at every refresh. Its grid is built from *spans*:
the workspace's minimum interval times a power of two. Such grids are nested:
every boundary of a span is a boundary of half that span. The grid takes the
first span that is not shorter than the cadence, keeps every boundary of it,
and keeps the midpoint between two boundaries for the share of slots that makes
the average spacing equal the cadence. Whether a midpoint is kept depends on a
hash of the route and the slot, not on earlier decisions.

- A cadence that changes a little changes only the midpoints whose hash lies
  between the old and the new share.
- A cadence that crosses into the next span changes nothing at the crossing:
  just below a span almost every midpoint of the doubled span is kept, and
  just above it almost none of its own.
- One route's points are a span or half a span apart: more than half the
  cadence and at most twice the cadence. They are never closer than the
  workspace's minimum interval.

Stateless alternatives with an exactly even spacing do not exist for a
changing cadence: an evenly spaced grid far from its origin moves by many
intervals when its spacing changes by a fraction of a percent. Storing each
workspace's phase in PostgreSQL would give even spacing, but every change
would have to reach all instances before any route already scheduled under
the old phase was dequeued, or those routes would miss or repeat a check. That
needs delayed activation of every cadence change. The nested grid needs no
shared state. Instances that hold snapshots of different ages agree on the
points of an automatic cadence, apart from the few midpoints between their two
shares. They do not agree on the points of a fixed interval or of a minimum
interval that was changed between their loads: see the consequences.

### A route is due at the earliest point of any owner

`checkpace.RouteCycle` holds one dequeue's decisions. The queue score is the
route's only scheduling state, and the cycle keeps it a watermark: every
owner's points before the score are settled (checked or skipped), and every
point from the score on is pending.

- **Who is checked.** An owner is due when one of its points lies between the
  route's due time and the dequeue. The due time was set to the earliest
  point any owner waited for, so no point lies before it, and the time a route
  waits in the queue is covered.
- **Who shares the check.** If at least one owner is due, the route is checked
  anyway. The cycle then also serves every owner whose next point comes before
  the horizon. That owner is checked ahead of its point, and the point is
  settled. Without this, an owner whose points do not coincide with another
  owner's would bring the route back for a check of its own between two checks
  the route gets anyway. The horizon is the route's next visit: the earliest
  next point of the owners that are due. An owner is served ahead for one
  point only, or it would lose checks. Its second point stays pending and
  brings the route back, so the horizon also ends at the earliest second point
  of the owners served ahead: no point settled ahead may lie at or after the
  score the route returns with.
- **When the route is due again.** At the earliest point any owner waits for.
  An owner the cycle checked waits for its first point after the cycle ended,
  and after the point it was served ahead for. A check that takes longer than
  the owner's interval therefore skips the points it overlapped and does not
  run back to back. An owner the cycle did not check still waits for its
  first point after the dequeue. If that point passed while the route was
  being checked for others, the due time lies in the past and the route is
  dequeued again at once. The owners that were just checked share that check
  if they reached a point in the meantime.
- **Revisits.** If no owner has a point within `MaxRouteInterval` (six hours),
  the route is revisited at its own revisit point instead. Revisit points are
  six hours apart and spread by route. A revisit checks nothing. It bounds how
  long an allowance refill, an upgrade or a shorter interval takes to reach a
  route, as the six-hour cap did.

Every owner gets one check per point of its own cadence, and the owners of a
route share its checks. Under the previous rule every dequeue served the
fastest owner and the others joined it, so a route was checked once per
interval of its fastest owner. This rule reaches the same rate where the
owners' grids are nested or regular: for fixed intervals in any combination,
and for automatic cadences with one minimum interval, a route is checked once
per point of the owner that has the most. Automatic cadences with different
minimum intervals, and automatic beside fixed ones, have points that one check
cannot always serve together, and a route then gets a few percent more checks
than that. The measurements below give the numbers.

Owners without allowance left neither are due nor hold the route. An owner the
snapshot lacks (before the first load, or a workspace created since the last
one) is checked by every cycle and holds the route at the hourly fallback, as
before.

The queue no longer computes a delay. `RequeueProxy` takes the due time from
the checker and writes it with the same completion script.

### Imports and explicit requeues

`AddToQueue` schedules a new route at the earliest point of its owners, which
spreads an import across the workspace's cadence. For a route that is already
queued, `add.lua` writes the earlier of the new due time and the one the route
waits at, in whichever shard it waits: the route keeps an earlier due time and
takes a sooner one. An import that adds a faster owner pulls the route
forward; no import postpones a route, also not one that moves it out of the
legacy queue or out of a shard of another shard count. A leased route is left
to its worker, as before.

`AddToQueueNow` queues a route at a due time before every point, so the next
dequeue checks it for every owner with allowance left. Only an instance
administrator's requeue of one route uses it. A workspace operator's lifecycle
change uses `AddToQueue`: it must not make a shared route due early, which
would spend the other owners' allowances.

Unlike an import, `AddToQueueNow` takes a route from a worker that is checking
it, in whichever shard the worker holds it. The worker's completion finds its
lease gone and does not reschedule the route, so the route stays due at once
for every owner. A request that arrived during a check would otherwise have
no effect. `add.lua` does this in the same commands: it schedules a leased
route like a waiting one when the caller asks for it. The administrator's
requeue still removes the route first, as it did before, which this no longer
depends on: the removal reaches only the route's current shard and the legacy
queue.

The administrator's requeue of all routes still spreads every route over the
fastest cadence, between 15 minutes and six hours. A route dequeued at such a
time is checked only if an owner has reached a point since that time, and
otherwise goes back to its owners' next points. Requeueing everything
therefore realigns the queue with the current cadences. It no longer forces a
check of every route, with its charge.

### Scraping

A source's fetches are still its fastest subscriber's interval apart, and a
slower subscriber is still served by the first fetch after each of its points.
Its points now come from the same grid, so a recomputed automatic scrape
cadence keeps a slower subscriber's fetches a span or half a span apart, to
within one fetch interval: exactly when the fetch interval divides the span.

### Refreshing allowances

Every import batch and every scrape requests a refresh, so a busy instance
loaded every workspace's allowance every five seconds, not every minute. A
requested load now waits at least five seconds and at least twenty times the
duration of the previous load after it, up to the regular minute. While a load
takes less than three seconds, requested loads take at most one twentieth of
the time, however many workspaces there are. A slower load is requested at
most once per minute, like the periodic refresh, which is unchanged. A request
that comes too early is carried out when the wait is over, not dropped.

The count of active routes whose tag rules change the number of protocols ran
one query per thousand such routes, inside the load, whenever the workspace's
route count changed. A workspace with more than 5,000 tag-rule routes is now
counted once per ten minutes. In between, its last count follows the active
route count in proportion, and the recomputed cadence corrects the estimate.
Smaller workspaces are counted exactly whenever their routes or tags changed,
as before.

One workspace's count must not keep every workspace's snapshot from being
refreshed, and no workspace may wait behind the same ones at every load:

- The counts that are due run before the allowances are built, the workspace
  whose last count or failed attempt is oldest first, and a workspace that was
  never counted before all others. A lifecycle write marks a workspace's
  count as out of date and leaves its place in that order alone, so a
  workspace whose routes change before every load does not take every turn.
- Counts start only during ten seconds, measured from the first count of a
  load, after the load's other queries. This holds for every count, also the
  exact ones of small workspaces. Every count has a ten-second limit of its
  own.
- A workspace whose count did not run, or failed, keeps its last count while
  its rules and defaults are the ones that were counted. A count made under
  other rules says nothing about the new ones. The effect is then estimated
  from the checker snapshot alone, as if every route whose plan adds
  protocols were active. Plans that remove protocols are left out of the
  estimate: their routes may be paused, and planning fewer checks per cycle
  than the workspace makes would make an automatic cadence too fast. The
  estimate therefore errs towards a slower cadence until the count has run.
- A count that fails is logged and repeated after ten minutes, so it does not
  use up the budget of every load.

The count and its cache stay in `database`, beside the lifecycle writes that
invalidate them inside the core's transactions.

## Hot-path operation count

Per proxy check, with plaintext queue payloads (the default):

| Category | Before | After |
| --- | --- | --- |
| Cryptographic operations | 0 | 0 |
| JSON encodes | 0 | 0 |
| Redis commands (pop, complete) | 2 | 2 |
| Database queries | unchanged | unchanged |

The cadence decisions remain in-memory arithmetic over the snapshot. The route
hash is computed once per dequeue. Per owner the cycle makes one snapshot map
read and one or two integer divisions for the due decision, and the same again
after the check for the owners that were checked. On a route where some owners
are due and others are not, it also computes the next point of every due owner
and a second point of the owners whose next one comes before the route's next
visit, from 24 bytes per owner of memory that the worker reuses. The base read
each owner twice more to compute the route's interval, at the dequeue and at
the requeue. A worker now marks the served owners in a reused mask instead of
copying them, which removes the one allocation of 608 bytes per owner that a
dequeue of a shared route made whenever one of its owners was not due.

Adding a route runs the same script with the same commands: it already read
the route's score in every shard, and now writes the earliest one with a plain
`ZADD` where it wrote with `ZADD NX`. Outside the check loop nothing is added:
no table, no key, no query.

## Measurements

### Scheduling

The same replay as in the table at the top, against the changed code. The
replay is a model of the queue in simulated time: each route is dequeued at
its due time, checked for the owners `RouteCycle` selects, and requeued at
the time `RouteCycle` returns. Automatic cadences were recomputed once per
simulated minute.

| Case | `57231ce` | This change |
| --- | --- | --- |
| Automatic, about 1 hour, sharing with a 5-minute owner | Median gap 40 minutes, 99th percentile 4.5 hours, longest 13 hours 50 minutes | Every gap 32 or 64 minutes to within 5 minutes; all planned checks made. The same with three instances that hold snapshots of different ages. |
| Automatic, about 96 hours, alone | Median 60 hours, 99th percentile 354 hours, longest 570 hours | Every gap 68 hours 16 minutes or 136 hours 32 minutes |
| Automatic, about 47 minutes, alone | Every gap the cadence plus the check time | Every gap 32 or 64 minutes |
| Fixed 1 hour and fixed 96 hours, alone | Every gap the interval plus the check time | Every gap the interval |
| Fixed 1 hour, sharing with a 1-minute owner, cycle time 10 s / 30 s / 90 s / 5 min | 86% / 67% / 39% / 17% of its checks | 100% / 100% / 99.995% / 100%, every gap one hour |
| 20,000 routes shared by a 5-minute and a 6-hour owner, after the 5-minute owner ran out | All 20,000 within 5 minutes, 72 times the even rate | The even rate; busiest minute 1.4 times the even rate |
| 20,000 routes of one workspace that changes 5 minutes to 6 hours | All routes in one 5-minute band every 6 hours, 4,000 per minute at the peak | Spread over the 6 hours; busiest minute 78 routes, even rate 56 |

These properties are held by the tests in
[route_test.go](../../internal/checkpace/route_test.go), which run the same
model for fewer routes.

### Shared checks

How often a shared route is checked decides how many outbound requests,
statistics and queue commands it costs. The previous rule checked a route once
per interval of its fastest owner. A first version of this change gave every
owner its own points and lost that: in the replay, a route with 1,000 owners
on a 1-minute interval was checked 992 times in one minute. With the grids
laid over each other and owners served ahead of a point that comes before the
route's next visit:

| Case | Checks of the route | Checks its owners got |
| --- | --- | --- |
| 1,000 owners on a 1-minute interval, same judge and protocol, 10 minutes | 10, with one request sequence each | 10,000 |
| 2,000 routes, a fixed 1-hour owner and a 5-minute owner, 20 days | 11,520,000, one per check of the 5-minute owner | 11,520,000 and 960,000 |
| The same with an automatic owner of about 1 hour | 11,520,000 | 11,520,000 and 959,932 |
| A fixed 90-minute owner and an automatic owner of about 91 minutes, 14 days | 237; the owner with the most points has 225 | 225 and 221 |

The last row is a case where one check cannot always serve both owners: the
automatic owner's points are 64 or 128 minutes apart, and two of them can fall
between two points of the fixed owner. Without the horizon rule that route
was checked 294 times.

For random sets of two to six owners, replayed for ten days, the checks of a
route per point of the owner that has the most were (400 sets per row; 1.000
is the least any rule could do, and the last column is what the route would
cost if nothing were shared):

| Owners | Median | 90th percentile | 99th percentile | Highest | Unshared, median / highest |
| --- | ---: | ---: | ---: | ---: | --- |
| Fixed intervals, also ones that do not divide each other | 1.000 | 1.000 | 1.000 | 1.000 | 1.50 / 4.70 |
| Automatic cadences with one minimum interval | 1.000 | 1.000 | 1.000 | 1.000 | 1.41 / 4.00 |
| Automatic cadences with minimum intervals of 1, 5 and 15 minutes | 1.000 | 1.059 | 1.142 | 1.778 | 1.52 / 4.41 |
| Fixed and automatic, mixed minimum intervals | 1.000 | 1.000 | 1.069 | 1.117 | 1.42 / 4.18 |

Sharing moves checks: an owner that is served ahead is checked before its
point. In the same replays, the time between two checks of one owner, as a
multiple of the shortest spacing of its own points, had a median of 1.0 in
every row. One gap in a hundred was shorter than 0.78 (fixed), 0.67 (mixed
minimum intervals) and 0.71 (fixed and automatic); the shortest single gaps
were 0.51, 0.13 and 0.36. With one minimum interval no gap was shorter than
the spacing.

`TestOwnersOnTheSameCadenceShareOneRequestSequence` runs the worker's own
selection and assignment builder for the first row of the first table, and
`TestOwnersOfARouteShareItsChecks` holds its last row and the fixed and
single-minimum cases. `TestRouteScoreStaysAWatermarkOfItsOwnersPoints`
checks, for 300 random sets of owners, check durations and queue delays, that
no owner is checked twice for one point and that no pending point is passed
over.

### Cost per dequeue

Six interleaved rounds (base, changed, base, ...) on one Linux amd64 machine
(i7-12700H, 20 logical CPUs, Go 1.27.4), while no other test run was active.
Base is `57231ce`. The base numbers are its due selection plus the interval
computation its requeue made, which is what the changed
`BenchmarkSelectDueWorkspaces` now covers in one `RouteCycle`: the selection
among all owners and the next due time. Both sides run one route whose owners
have automatic cadences of 5, 10, 15, ... minutes and dequeue it at its
fastest owner's cadence, so every owner is examined and a few are served in
each iteration.

| Owners of the route | Base median ns | Changed median ns | Range base / changed | Memory base / changed |
| ---: | ---: | ---: | --- | --- |
| 1 | 57.9 | 75.9 | 53-60 / 70-77 | 0 B, 0 allocs / 0 B, 0 allocs |
| 8 | 966.6 | 485.4 | 916-1,086 / 448-490 | 4,864 B, 1 alloc / 0 B, 0 allocs |
| 100 | 12,520.0 | 4,945.0 | 11,263-21,106 / 4,531-5,199 | 65,536 B, 1 alloc / 0 B, 0 allocs |
| 1,000 | 125,925.5 | 51,438.5 | 116,135-165,190 / 46,806-51,584 | 589,824 B, 1 alloc / 0 B, 0 allocs |

A route with one owner costs 18 ns more per dequeue: the base added the
owner's interval to the completion time, and the change computes the owner's
next point, which is one hash and two integer divisions more. That is under
0.02% of the roughly 130 microseconds the two Redis round trips of a cycle
take on the same machine, before the check itself. A shared route costs about half
or less of what it did and no longer allocates. The benchmark's owners share
one minimum interval, so it does not cover routes whose owners are served
ahead at every dequeue; those cost one more point computation per such owner.

The benchmarks around the decision did not change beyond their run-to-run
range, and none allocates more:

| Benchmark | Base median ns/op | Changed median ns/op | Range base / changed | Memory |
| --- | ---: | ---: | --- | --- |
| BenchmarkRequestMeterAdmit/owners=1 | 62.8 | 63.9 | 60-66 / 60-65 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=8 | 223.8 | 223.9 | 221-230 / 213-229 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=64 | 2,035.5 | 2,022.5 | 1,859-2,097 / 1,936-2,099 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=512 | 17,731.0 | 18,282.5 | 15,272-18,585 / 17,257-19,190 | 0 B, 0 allocs both |
| BenchmarkChargeRequest | 16.7 | 16.7 | 16-19 / 16-17 | 0 B, 0 allocs both |
| BenchmarkBuildRequestAssignmentsSharedRoute | 446,041.0 | 469,447.0 | 424,602-514,256 / 455,290-486,053 | 4,038 allocs both |
| BenchmarkProxyQueueCycle | 135,089.5 | 130,643.5 | 122,839-174,371 / 123,222-148,455 | 47 allocs both |
| BenchmarkProxyQueueCycleConcurrent | 70,245.0 | 45,987.0 | 44,934-86,580 / 42,659-46,857 | 48 allocs both |

`BenchmarkBuildRequestAssignmentsSharedRoute` is 5.2% slower in the median
with the changed range inside the base's; two earlier runs of the same pair
gave +1.7% and -3.1%. It reads the selection through one method call per
owner, where the base indexed a slice of the due owners. The two queue
benchmarks ran against a disposable Redis with `-benchtime=20000x`. Their
commands are the same on both sides; the base's concurrent rounds varied by a
factor of 1.9, so the lower median is not a gain of this change.

The scale tests on matched hardware have not been run for this change.

## Consequences

- One route's checks on an automatic cadence are a span or half a span apart
  instead of exactly the cadence. For a single owner faster than six hours
  that is less even than before; for a slower owner or a slower co-owner it
  replaces random sampling. The workspace still uses its allowance at the
  published cadence.
- On a shared route an owner can be checked ahead of its point, by less than
  the time to the route's next visit, which is at most the longest gap
  between two points of an owner that is due at that dequeue. With a 5-minute
  co-owner, an automatic cadence of about one hour is checked 32 or 64
  minutes apart to within 5 minutes. Owners whose cadences are close to each
  other and do not divide each other, such as 9, 10 and 11 minutes, are
  checked together: each still gets one check per point of its own, but two
  of its checks can be closer together or further apart than its points are,
  and closer than its minimum interval. The measurements give the
  distribution.
- A route is checked once per point of the owner that has the most only where
  the owners' grids allow it. Automatic cadences with different minimum
  intervals, and automatic beside fixed ones, cost a few percent more checks
  of the route than the previous rule's one per interval of the fastest
  owner: up to 14% for 99 of 100 random sets of owners, and 78% for the worst
  one measured. Capacity planning for instances with many such routes should
  measure this on its own data.
- A fixed interval is kept on the clock, from the start of one check to the
  start of the next, not from the end of one to the start of the next. A
  fixed-interval workspace now makes the checks its interval implies, which is
  what the run-out estimate and the worker sizing already assumed. Before, it
  made fewer by the share of the interval that its checks and the queue took.
  An instance whose workspaces use fixed intervals therefore runs more checks
  after this change: about 17% more for a 1-minute interval whose checks take
  10 seconds, and about 0.3% more for a 1-hour interval.
- A point that an owner reaches while the route is being checked for other
  owners is served by the cycle right afterwards. The owners that were just
  checked share it if they reached a point during their check, so a point
  that falls into an owner's own check is skipped only when no other owner
  brings the route back at once.
- Instances must agree on a workspace's points. They do for an automatic
  cadence, whatever the age of their snapshots. They do not while one instance
  has loaded a changed fixed interval or minimum interval and another has not:
  a route that one of them scheduled is then usually not at a point for the
  other, which checks nothing and puts the route on its own points. While
  that lasts the workspace gets fewer checks: none in the worst case, where
  the two instances take strict turns on a route (measured: 2 checks instead
  of about 118 in two hours for 60 against 61 seconds). It ends with the next
  load, normally within a minute, and lasts as long as an instance cannot
  load allowances. The previous rule checked the fastest owner at every
  dequeue and had no such dependency. Recognising the points of another
  snapshot needs a transition rule for cadence changes, which this decision
  does not include.
- The first dequeue of a route after this change is deployed usually checks
  nothing and puts the route on its owners' points, because the due time the
  previous rule wrote is not one of them. That check then arrives up to one
  interval later than it would have. A route is checked at that dequeue if an
  owner reached a point while the route waited in the queue, if the snapshot
  lacks an owner, or if it was requeued for every owner.
- A route whose worker died during a check is reclaimed when its lease
  expires and realigned in the same way. The owners whose check was lost wait
  for their next point instead of being checked at once.
- A new route waits for its owner's first point, also when it was imported
  alone. Before, the first route of every import batch was due at once.
- A workspace that an import adds to a route while a worker holds it is seen
  at that route's next dequeue.
- A workspace whose allowance was used up when the snapshot was taken has no
  points until the next load, also in the first minute of a new month.
- The scraper keeps its fetch-window rule. The time a fetch takes is short
  against the 15-minute minimum, so the window gap stays negligible there.
- For a workspace with more than 5,000 tag-rule routes, a tag assignment or a
  pause changes the planned checks per cycle when the workspace is counted
  again: ten minutes after its last count at the earliest, later while more
  counts are due than fit into the loads, and ten minutes after a count that
  failed. Until then the estimate follows the route count only.
- The first load after a start has no counts yet. Workspaces whose count does
  not fit into its ten seconds start on the estimate from their checker
  snapshot and are counted by the following loads.
- ADR 0002 is unaffected. The queue payload format and plaintext default have
  not changed.
