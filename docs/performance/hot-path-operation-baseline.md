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

Workers wait for two process-wide batchers. Each collects requests for 50 ms or
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
go test ./internal/jobs/checker -run 'TestOwnershipVerifierBatchesConcurrentRequests|TestFailureTrackerBatchesConcurrentRequests|TestCheckerBatchersAreProcessSingletons' -count=1 -v
```

The queue command-budget test needs `MAGPIE_TEST_QUEUE_REDIS_URL`; without it
the test skips.

## Changes since the baseline

Every change to a pinned hot-path package is recorded here with its effect on
the counts above.

| Change | Cryptographic operations | JSON encodes | Redis commands | Database queries |
| --- | --- | --- | --- | --- |
| The shared pool handle moved from `database.DB` to `pg.DB`. The failure tracker and the ownership verifier read the same handle under its new qualifier; `jobs/checker` now imports `platform/pg`. | +0 | +0 | +0 | +0 |
