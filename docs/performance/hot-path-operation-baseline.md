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
| Check usage metering | 0 Redis, 0 database | `Snapshot.ChargeRequest` updates in-memory counters, once per workspace per request (`R` times the workspaces served). |

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
The check usage meter writes one batch about every ten seconds per instance.

## Tests that hold these numbers

| Number | Test |
| --- | --- |
| Queue commands `evalsha, evalsha`, payload not rewritten | `TestPlaintextProxyCycleCommandBudgetAndPayload` in [proxy_queue_concurrency_test.go](../../internal/jobs/queue/proxy/proxy_queue_concurrency_test.go) |
| Stored hash reused without the cipher | `TestQueuedProxyToDomainProxy_CurrentPlaintextPayloadReusesHashWithoutEncryptionKey` in [proxy_queue_test.go](../../internal/jobs/queue/proxy/proxy_queue_test.go) |
| Normal requeue writes no payload | `TestRequeueProxy_OnlyPersistsPayloadWhenRequested` in [proxy_queue_test.go](../../internal/jobs/queue/proxy/proxy_queue_test.go) |
| Ownership lookups share `ceil(K/8192)` statements | `TestOwnershipVerifierBatchesConcurrentRequests` in [batcher_composition_test.go](../../internal/jobs/checker/batcher_composition_test.go) |
| Failure tracking shares chunked statements | `TestFailureTrackerBatchesConcurrentRequests` in [batcher_composition_test.go](../../internal/jobs/checker/batcher_composition_test.go) |
| Statistics persistence statement counts | `TestCheckerEvidencePersistenceOperationBudgetPostgres` in [checker_settings_performance_test.go](../../internal/database/checker_settings_performance_test.go) |

```bash
go test ./internal/jobs/queue/proxy -run 'TestPlaintextProxyCycleCommandBudgetAndPayload|TestQueuedProxyToDomainProxy_CurrentPlaintextPayloadReusesHashWithoutEncryptionKey|TestRequeueProxy_OnlyPersistsPayloadWhenRequested' -count=1 -v
go test ./internal/jobs/checker -run 'TestOwnershipVerifierBatchesConcurrentRequests|TestFailureTrackerBatchesConcurrentRequests|TestCheckerRuntimeOwnsOneOfEachBatcher|TestStoppedBatchersProcessReceivedRequests' -count=1 -v
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

