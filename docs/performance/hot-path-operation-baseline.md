# Per-check operation counts of the checker loop

This is the operation accounting the workspace `AGENTS.md` requires for proxy
queue and checker changes. It was read from the code at `3f66f5d` and is the
baseline every later change is compared with. A change that alters a number
here updates this file in the same diff and gives its measurement and reason.

One **cycle** is one claimed route: `work` in
[thread_handler.go](../../internal/jobs/checker/thread_handler.go). A cycle
sends one outbound request sequence per **assignment**; workspaces that share
judge, protocol, transport, timeout and retry budget share one assignment.
`A` is the number of assignments in the cycle, and `R` the number of outbound
requests including retries.

## Default configuration

`PROXY_QUEUE_ENCRYPT_CREDENTIALS=false`, Redis statistics stream enabled,
overload policy `block`, lease longer than the check.

| Category | Per cycle | Where |
| --- | --- | --- |
| Cryptographic operations | 0 | A current plaintext payload carries its route hash; `toDomainProxy` copies it. No cipher, nonce or fingerprint. |
| JSON encodes | `A` | One `json.Marshal` of the statistic per assignment in `enqueueProxyStatisticToStream`. Requeueing encodes nothing. |
| JSON decodes | 1 | The claimed payload in `GetNextProxyContext`. |
| Redis commands, queue | 2 | `EVALSHA` `pop.lua` to claim and `EVALSHA` `complete.lua` to requeue. |
| Redis commands, statistics | `2A` | `XLEN` and `XADD` per statistic under `block` or `drop_new` with a stream cap. `evict_oldest` sends only `XADD`. |
| Database queries issued by the worker | 0 | The worker issues none directly. |
| Check usage metering | 0 Redis, 0 database | `Pacer.ChargeRequest` on the runtime's Pacer updates in-memory counters, once per workspace per request (`R` times the workspaces served). |
| Cadence decisions | 0 Redis, 0 database, 0 allocations | A `checkpace.RouteCycle` on the worker's stack decides which owners are served and when the route is due again, from the in-memory snapshot: one hash per route, and per owner one map read and a few integer divisions. Owners share the route's checks: a shared route is claimed once per cadence point of the owner that has the most, or a few percent more often where the owners' grids are not nested (see ADR 0016). |

Lease renewal sends one more `EVALSHA` (`renew.lua`) between attempts only when
less than two minutes of the five-minute lease remain. An ownership change
replaces `RequeueProxy` with `RequeueProxyWithPayload`: one JSON encode more
and the same single `EVALSHA`.

## Shared database batches

Workers wait for two shared batchers, owned by the process's `checker.Runtime`
and started and stopped by `app/bootstrap`. Each collects requests for 50 ms or
until 8,192 are waiting, then issues statements for the whole batch.

| Batcher | Statements per batch of `K` cycles | Where |
| --- | --- | --- |
| Failure tracker | `ceil(S/200)` streak resets and `ceil(F/200)` streak increments, where `S` and `F` are the batch's successful and failed workspace results. A reached threshold adds that workspace's pause or delete action. | [failure_tracker.go](../../internal/jobs/checker/failure_tracker.go) |
| Ownership verifier | 1 `SELECT DISTINCT proxy_id` for at most 8,192 routes, so `ceil(K/8192)` | [ownership_verifier.go](../../internal/jobs/checker/ownership_verifier.go) |

Statistics reach PostgreSQL asynchronously in batches of up to 5,000 results or
15 seconds per consumer, in one `InsertProxyStatistics` transaction per batch.
The check usage meter, the process's `checkpace.Pacer`, writes one batch about
every ten seconds per instance.

## Tests that hold these numbers

| Number | Test |
| --- | --- |
| Queue commands `evalsha, evalsha`, payload not rewritten | `TestPlaintextProxyCycleCommandBudgetAndPayload` in [proxy_queue_concurrency_test.go](../../internal/jobs/queue/proxy/proxy_queue_concurrency_test.go) |
| Stored hash reused without the cipher | `TestQueuedProxyToDomainProxy_CurrentPlaintextPayloadReusesHashWithoutEncryptionKey` in [proxy_queue_test.go](../../internal/jobs/queue/proxy/proxy_queue_test.go) |
| Normal requeue writes no payload | `TestRequeueProxy_OnlyPersistsPayloadWhenRequested` in [proxy_queue_test.go](../../internal/jobs/queue/proxy/proxy_queue_test.go) |
| Ownership lookups share `ceil(K/8192)` statements | `TestOwnershipVerifierBatchesConcurrentRequests` in [batcher_composition_test.go](../../internal/jobs/checker/batcher_composition_test.go) |
| Failure tracking shares chunked statements | `TestFailureTrackerBatchesConcurrentRequests` in [batcher_composition_test.go](../../internal/jobs/checker/batcher_composition_test.go) |
| Statistics persistence statement counts | `TestCheckerEvidencePersistenceOperationBudgetPostgres` in [checker_settings_performance_test.go](../../internal/database/checker_settings_performance_test.go) |
| Metering charges each served workspace once per request, without allocating | `TestRequestMeterChargesEachWorkspaceOncePerRequest` and `TestRequestMeterDoesNotAllocateWhileAllowanceLasts` in [check_pace_test.go](../../internal/jobs/checker/check_pace_test.go) |
| A cycle's cadence decisions allocate nothing | `TestDueSelectionServesAllOwnersOfASingleOwnerRouteWithoutAllocating` in [check_pace_test.go](../../internal/jobs/checker/check_pace_test.go) |
| Owners of a route share its request sequences: a thousand owners on one interval cost one sequence per interval | `TestOwnersOnTheSameCadenceShareOneRequestSequence` in [check_pace_test.go](../../internal/jobs/checker/check_pace_test.go) |
| A shared route is claimed once per point of the owner that has the most (at most 10% more often for a fixed beside an automatic cadence), and its queue score stays a watermark of its owners' points | `TestOwnersOfARouteShareItsChecks` and `TestRouteScoreStaysAWatermarkOfItsOwnersPoints` in [route_test.go](../../internal/checkpace/route_test.go) |
| A requeue writes exactly the due time the checker decided | `TestRequeueProxy_OnlyPersistsPayloadWhenRequested` in [proxy_queue_test.go](../../internal/jobs/queue/proxy/proxy_queue_test.go) and `TestRequeueUsesWorkspaceAllowanceCadence` in [queue_interval_test.go](../../internal/app/bootstrap/queue_interval_test.go) |
| One Pacer per process: constructed in `app`, shared by both queues, charged only by its own checker runtime | `TestOnlyAppConstructsAPacer` and `TestInstallPacedQueuesGivesBothQueuesTheProcessPacer` in [composition_test.go](../../internal/app/composition_test.go); `TestCheckerRuntimeChargesOnlyItsOwnPacer` in [check_pace_test.go](../../internal/jobs/checker/check_pace_test.go) |
| The meter's final flush follows the checker workers and the batchers | `TestPacerFinalFlushFollowsCheckerWorkersAndBatchers` in [lifecycle_test.go](../../internal/app/bootstrap/lifecycle_test.go) |

```bash
go test ./internal/jobs/queue/proxy -run 'TestPlaintextProxyCycleCommandBudgetAndPayload|TestQueuedProxyToDomainProxy_CurrentPlaintextPayloadReusesHashWithoutEncryptionKey|TestRequeueProxy_OnlyPersistsPayloadWhenRequested' -count=1 -v
go test ./internal/jobs/checker -run 'TestOwnershipVerifierBatchesConcurrentRequests|TestFailureTrackerBatchesConcurrentRequests|TestCheckerRuntimeOwnsOneOfEachBatcher|TestStoppedBatchersProcessReceivedRequests|TestRequestMeter|TestCheckerRuntimeChargesOnlyItsOwnPacer|TestDueSelectionServesAllOwnersOfASingleOwnerRouteWithoutAllocating|TestOwnersOnTheSameCadenceShareOneRequestSequence' -count=1 -v
go test ./internal/checkpace -run 'TestOwnersOfARouteShareItsChecks|TestRouteScoreStaysAWatermarkOfItsOwnersPoints' -count=1 -v
go test ./internal/app ./internal/app/bootstrap -run 'TestOnlyAppConstructsAPacer|TestInstallPacedQueuesGivesBothQueuesTheProcessPacer|TestPacerFinalFlushFollowsCheckerWorkersAndBatchers|TestRequeueUsesWorkspaceAllowanceCadence' -count=1 -v
```

The queue command-budget test needs `MAGPIE_TEST_QUEUE_REDIS_URL`; without it
the test skips.

## Changes since the baseline

Every change to a pinned hot-path package is recorded here with its effect on
the counts above.

| Change | Cryptographic operations | JSON encodes | Redis commands | Database queries |
| --- | --- | --- | --- | --- |
| The shared pool handle moved from `database.DB` to `pg.DB`. The failure tracker and the ownership verifier read the same handle under its new qualifier; `jobs/checker` now imports `platform/pg`. | +0 | +0 | +0 | +0 |
| The two checker batchers moved from `init()` singletons to a `checker.Runtime` that `app/bootstrap` constructs, starts in its own routine group and stops after the checker workers and before the consumers. Workers reach the batchers through their runtime instead of a package variable; batch size, window and statements are unchanged. | +0 | +0 | +0 | +0 |
| `CountActiveInstances` and the heartbeat index helpers moved from `jobs/runtime` to `instanceid`. `jobs/queue/proxy` imports `instanceid` instead of `jobs/runtime`, so neither queue depends on `internal/database` any more. The function runs when the checker and scraper size their workers (every 15 s), never per check, and sends the same Redis commands. | +0 | +0 | +0 | +0 |
| The check usage meter and its snapshot moved from `checkpace` package variables to a `checkpace.Pacer` that `app` constructs once and passes to both queues, `app/bootstrap` (which configures and runs it among the consumers and gives it to the checker runtime and the scraper dispatcher) and the API. Per cycle, the worker reads the snapshot with `r.pace.Current()`, one atomic load as before, and calls `RouteCheckInterval`, `CheckDue` and `ChargeRequest` as methods of that `*Pacer` with the snapshot as an argument. The base reached the snapshot and the charge through two package-level function variables (`currentPace`, `chargeRequest`), so each request now makes two fewer indirect calls; no interface call is added. The requeue reads the queue's Pacer instead of the package variable. A nil snapshot still means "before the first load": every owner is due and routes use the fallback interval. | +0 | +0 | +0 | +0 |

| Routes are scheduled on workspace cadence grids ([ADR 0016](../adr/0016-schedule-routes-on-workspace-cadence-grids.md)). The worker opens one `checkpace.RouteCycle` per claimed route, which decides the due owners and the route's next due time; `RequeueProxy` takes that time and no longer computes an interval, so the snapshot is read once per cycle instead of at the dequeue and again at the requeue. The served owners are marked in a mask the worker reuses, not copied. Owners still share a route's checks: their grids are laid over each other, and a cycle that checks a route also serves the owners whose next point comes before the route's next visit, so a shared route is claimed once per point of the owner that has the most, as before, or a few percent more often where the owners' grids are not nested. `pop.lua` and `complete.lua` are unchanged; `add.lua`, which runs per import and not per check, issues the same commands, writes the earliest due time with a plain `ZADD` where it used `ZADD NX`, and for an explicit requeue schedules a leased route like a waiting one. | +0 | +0 | +0 | +0 |

### Measurement of the cadence grid change

Six interleaved rounds (base, changed, base, ...) on one Linux amd64 machine
with 20 logical CPUs, while no other test run was active. Base is `57231ce`.
The base's `BenchmarkSelectDueWorkspaces` measured the due selection only, so
the base side below adds the `RouteCheckInterval` call its requeue made: both
sides measure all cadence work of one cycle, on the same route, whose owners
have automatic cadences of 5, 10, 15, ... minutes.

| Benchmark | Base median ns/op | Changed median ns/op | Change | Range base / changed | Memory base / changed |
| --- | ---: | ---: | ---: | --- | --- |
| Cadence decisions of one cycle, owners=1 | 57.9 | 75.9 | +31.1% | 53-60 / 70-77 | 0 B, 0 allocs both |
| Cadence decisions of one cycle, owners=8 | 966.6 | 485.4 | -49.8% | 916-1086 / 448-490 | 4864 B, 1 alloc / 0 B, 0 allocs |
| Cadence decisions of one cycle, owners=100 | 12520.0 | 4945.0 | -60.5% | 11263-21106 / 4531-5199 | 65536 B, 1 alloc / 0 B, 0 allocs |
| Cadence decisions of one cycle, owners=1000 | 125925.5 | 51438.5 | -59.2% | 116135-165190 / 46806-51584 | 589824 B, 1 alloc / 0 B, 0 allocs |
| BenchmarkRequestMeterAdmit/owners=1 | 62.8 | 63.9 | +1.8% | 60-66 / 60-65 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=8 | 223.8 | 223.9 | +0.0% | 221-230 / 213-229 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=64 | 2035.5 | 2022.5 | -0.6% | 1859-2097 / 1936-2099 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=512 | 17731.0 | 18282.5 | +3.1% | 15272-18585 / 17257-19190 | 0 B, 0 allocs both |
| BenchmarkChargeRequest | 16.7 | 16.7 | +0.3% | 16-19 / 16-17 | 0 B, 0 allocs both |
| BenchmarkBuildRequestAssignmentsSharedRoute | 446041.0 | 469447.0 | +5.2% | 424602-514256 / 455290-486053 | 4038 allocs both |
| BenchmarkProxyQueueCycle (`-benchtime=20000x`) | 135089.5 | 130643.5 | -3.3% | 122839-174371 / 123222-148455 | 47 allocs both |
| BenchmarkProxyQueueCycleConcurrent (`-benchtime=20000x`) | 70245.0 | 45987.0 | -34.5% | 44934-86580 / 42659-46857 | 48 allocs both |

A route with one owner costs 18 ns more per cycle. The base added the owner's
interval to the completion time. The change computes the owner's next cadence
point, one hash and two integer divisions more, because a due time on the
grid is what keeps a recomputed automatic cadence regular and spreads a
workspace's routes after its interval changed. That is under 0.02% of the two
Redis round trips of the same cycle. A shared route costs about half or less
and no longer allocates: the base copied the due owners into a new slice, 608
bytes per owner, whenever not all of them were due.

How often a shared route is claimed is what its owners' checks cost in
requests, statistics and queue commands. Owners share a route's checks as
before: a route with a thousand owners on a one-minute interval costs one
claim and one request sequence per minute
(`TestOwnersOnTheSameCadenceShareOneRequestSequence`), and in a replay of
2,000 routes owned by a 5-minute and a 1-hour workspace for 20 days the
routes were claimed 11,520,000 times for the 11,520,000 checks of the
5-minute workspace, which the 960,000 checks of the other all shared. For
fixed intervals in any combination and for automatic cadences with one
minimum interval, a route is claimed once per point of the owner that has the
most. Automatic cadences with different minimum intervals, or beside fixed
ones, cannot always share one check: in 400 random sets of owners each, the
median was still one claim per such point, 99 of 100 sets stayed within 14%
of it, and the worst was 78% above it. ADR 0016 has the table.

`BenchmarkBuildRequestAssignmentsSharedRoute` is 5.2% slower in the median
with the changed range inside the base's; two earlier runs of the same pair
gave +1.7% and -3.1%. The queue benchmarks send the same commands on both
sides, and the base's concurrent rounds varied by a factor of 1.9, so its
lower median is not a gain of this change.

### Measurement of the check usage meter ownership change

Interleaved rounds (base, changed, base, ...) on the same machine, while no
other test run was active: six rounds of every benchmark below, then ten
more of the two whose medians moved most. Base is `189f40b` with its own
benchmarks, written against the package functions; the changed tree runs the
same benchmarks against a `Pacer`. `BenchmarkProxyQueueCycle` uses a
disposable Redis database.

| Benchmark | Base median ns/op | Changed median ns/op | Change | Range base / changed | Memory |
| --- | ---: | ---: | ---: | --- | --- |
| BenchmarkChargeRequest (16 rounds) | 15.8 | 16.4 | +3.7% | 15-17 / 15-17 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=1 | 65.4 | 64.8 | -0.9% | 60-73 / 55-65 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=8 | 229.4 | 223.9 | -2.4% | 216-234 / 220-225 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=64 | 2051.5 | 1997.0 | -2.7% | 1941-2135 / 1877-2064 | 0 B, 0 allocs both |
| BenchmarkRequestMeterAdmit/owners=512 | 18808.5 | 16595.0 | -11.8% | 15654-19119 / 14774-18161 | 0 B, 0 allocs both |
| BenchmarkSelectDueWorkspaces/owners=1 | 51.4 | 49.2 | -4.2% | 49-52 / 47-51 | 0 B, 0 allocs both |
| BenchmarkSelectDueWorkspaces/owners=8 | 861.2 | 861.9 | +0.1% | 832-936 / 823-898 | 4864 B, 1 alloc both |
| BenchmarkProxyQueueCycle (10 extra rounds) | 123006.0 | 126117.5 | +2.5% | 110454-133404 / 108124-137288 | 47 allocs both |
| BenchmarkProxyQueueCycleConcurrent | 71718.5 | 71346.0 | -0.5% | 47821-78294 / 46209-77498 | 48 allocs both |
| BenchmarkBuildRequestAssignmentsSharedRoute | 462088.5 | 450599.0 | -2.5% | 452579-480136 / 408384-491672 | 4038 allocs both |
| BenchmarkCheckerBatchedCycle | 1645653.0 | 1645657.0 | +0.0% | 1643053-1652767 / 1644155-1651838 | |
| BenchmarkProxyCheckRequest/2048 | 5572.0 | 5612.5 | +0.7% | 5544-11721 / 5552-5993 | 93 allocs both |
| BenchmarkProxyCheckRequest/65536 | 37839.0 | 40742.5 | +7.7% | 13077-43014 / 13266-42664 | 105 allocs both |

`ChargeRequest` alone is about 0.6 ns slower per call in every round: it
reaches the pause clock and the month cache through the Pacer instead of
package variables, behind one more nil check. Neither helper is inlined in
either version, and splitting them did not bring them under the inlining
budget, so the split was not kept. The checker never calls `ChargeRequest`
on its own: `BenchmarkRequestMeterAdmit`, which is the checker's
per-request path and includes one charge per owner, is not slower at any
owner count, because the base reached the snapshot and the charge through
function variables. The queue cycle's first six rounds showed +6.1% and the
ten further rounds +2.5%, with the ranges overlapping in both; its change is
one atomic load through the queue's Pacer against about 120 microseconds of
Redis round trips. `BenchmarkProxyCheckRequest/65536` varies by a factor of
three within each side, as before. No benchmark allocates more.

### Measurement of the checker batcher ownership change

Six interleaved rounds (base, changed, base, ...) on one Linux amd64 machine
with 20 logical CPUs, while no other test run was active. Base is `f5baa1f`
with the same batched-cycle benchmark written against the old package
functions. `BenchmarkCheckerBatchedCycle` runs 64 concurrent workers that each
submit a failure-tracking event and an ownership check on SQLite, so it is
dominated by the 50 ms batch window; it exercises the changed path. The
others cover the checker loop around it and did not change.

| Benchmark | Base median ns/op | Changed median ns/op | Change | Range base / changed, memory |
| --- | ---: | ---: | ---: | --- |
| BenchmarkBuildRequestAssignmentsSharedRoute | 449078.0 | 458207.0 | +2.0% | 420550-471415 / 419697-482377 | 422368 -> 422368 B/op, 4038 -> 4038 allocs/op |
| BenchmarkCheckerBatchedCycle | 1645849.0 | 1647209.0 | +0.1% | 1640715-1654025 / 1644845-1652618 |
| BenchmarkProxyCheckRequest/2048 | 5674.5 | 5607.0 | -1.2% | 5546-7769 / 5563-7267 | 15826 -> 15826 B/op, 93 -> 93 allocs/op |
| BenchmarkProxyCheckRequest/65536 | 41696.0 | 41636.0 | -0.1% | 13232-44180 / 13330-43570 | 92222 -> 92202 B/op, 105 -> 105 allocs/op |
| BenchmarkRequestMeterAdmit/owners=1 | 64.4 | 60.6 | -5.8% | 59-65 / 58-65 | 0 -> 0 B/op, 0 -> 0 allocs/op |
| BenchmarkRequestMeterAdmit/owners=512 | 18912.5 | 18885.5 | -0.1% | 15779-19317 / 17681-20963 | 0 -> 0 B/op, 0 -> 0 allocs/op |
| BenchmarkRequestMeterAdmit/owners=64 | 2058.5 | 2032.5 | -1.3% | 1867-2184 / 1765-2162 | 0 -> 0 B/op, 0 -> 0 allocs/op |
| BenchmarkRequestMeterAdmit/owners=8 | 228.1 | 234.2 | +2.7% | 219-235 / 224-234 | 0 -> 0 B/op, 0 -> 0 allocs/op |
| BenchmarkSelectDueWorkspaces/owners=1 | 51.8 | 51.8 | +0.1% | 47-52 / 48-52 | 0 -> 0 B/op, 0 -> 0 allocs/op |
| BenchmarkSelectDueWorkspaces/owners=8 | 888.1 | 872.2 | -1.8% | 810-916 / 839-929 | 4864 -> 4864 B/op, 1 -> 1 allocs/op |

Every difference is inside the run-to-run range of both sides, and allocations
are identical. `BenchmarkProxyCheckRequest/65536` varies by a factor of three
within each side.
