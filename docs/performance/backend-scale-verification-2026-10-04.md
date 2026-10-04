# Backend scale fix verification

Reviewed the current uncommitted fixes over backend commit
`9c3e77e3ac9dfdf8319e08eb2cfcbfc5618fcc43` on 2026-10-04.

The eleven findings have corresponding changes and regression coverage. Ten
original findings pass verification for their reported failure scenarios.
Lease renewal fixes ordinary long checks, but finding 10 remains incomplete:
bulk requeue and imports from a legacy shard can still start duplicate work.
An additional cancellation issue remains in statistics acknowledgement.

No runtime code was changed during this verification. Independent tests use
Go's file overlay mechanism and live outside the repository.

## Remaining issues

### 1. Bulk requeue invalidates active leases without stopping their workers

Sources: [bulk scheduling rewrite](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:1010),
[renewal fast path](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:749),
[retry loop](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/thread_handler.go:585).

`POST /api/global/proxies/requeue` calls `RequeueAll`, which writes new due
times over every member's score, including active processing leases. Another
worker can immediately acquire a member whose new due time has arrived. The
original worker's `RenewLeaseIfNeeded` returns success without checking Redis
while its remembered expiry is more than two minutes away.

A real Redis reproduction dequeued a route, called `RequeueAll`, and dequeued
the same route again before its original five-minute lease expired. A separate
checker reproduction acquired a second lease during the first worker's first
attempt. The original worker then made all three attempts instead of returning
`ErrProxyLeaseLost`.

Completion fencing cannot undo those duplicate requests. The checker also
calls the same renewal fast path before failure tracking, so it does not
prevent stale failure processing in this case. That latter consequence follows
from the code; the independent checker test measured retry attempts.

Preserve active lease ownership when bulk scheduling changes. Any intended
revocation needs to stop the old worker before it can keep producing results
or failure actions. Keep the normal checker command budget intact and measure
any additional hot-path operations.

### 2. Importing an active legacy-shard route creates a second lease

Sources: [enqueue into the current shard](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:284),
[legacy member removal](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:293).

A current-format payload can still reside in the supported `proxy_queue`
legacy sorted set. If a worker has leased it there, `AddToQueue` adds it to the
current shard with a fresh due time and removes its legacy member. `NX` only
checks the destination shard, so it does not preserve the existing lease.

The real Redis reproduction added a second workspace while the legacy worker
was active. Another dequeue immediately returned the same route with both
workspaces. The original worker's renewal fast path still returned success.
The new test for completing a current payload from a legacy shard passes, but
does not exercise an import during that check.

Import and shard migration must preserve an existing active lease atomically,
including when its member lives in a different supported shard.

### 3. Statistics acknowledgement retries ignore cancellation

Sources: [retry before checking cancellation](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:472),
[coordinator waits for workers](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:269).

After PostgreSQL commits, the buffer is empty but message IDs remain until
Redis acknowledges them. If cancellation interrupts `XACK`, the loop keeps
retrying with the canceled context, sleeps, and continues before reaching its
`ctx.Done()` branch. The statistics coordinator waits for this worker to exit.

An independent test canceled the worker on its first acknowledgement against
real Redis and PostgreSQL. The history row committed once, but the worker did
not stop and logged `context canceled` on every retry. The same loop structure
exists before this fix set, so this is an additional pre-existing issue rather
than a regression introduced by the changes.

Check cancellation before retrying persistence or acknowledgement. Leave
unacknowledged messages replayable, or use a bounded shutdown attempt with a
fresh context. The new event ledger protects committed messages from repeated
accounting on later replay.

## Original finding verification

| Finding | Result | Evidence |
| --- | --- | --- |
| 1. Pending recovery repeats messages | Verified | Existing worker tests pass with one and 37 events. Independent PostgreSQL/Redis tests pass with 5,001 and 10,037 pending events across multiple pages. |
| 2. Replay double-counts committed events | Verified | PostgreSQL and SQLite tests cover duplicates within a batch, later replay, history deletion, rollback, and concurrent consumers. |
| 3. Rotators load entire pools | Verified | SQL counts replace pool loading in creation and listing. The 65,536-route PostgreSQL regression passes. |
| 4. Usage writes recount inventory | Verified | The inventory count is removed from check usage writes. Capacity preservation, month rollover, and one-million-route regressions pass. |
| 5. Client eviction scans the map | Verified | LRU eviction replaces map scans. Hits precede eviction, expiration cleanup is bounded, and cache regressions pass. |
| 6. Uptime scans unrelated history | Verified | History aggregation correlates to the candidate route. The regression visits one matching history row with 200,000 unrelated rows present. |
| 7. Source counts recount on each route update | Verified | Notifications coalesce source/workspace pairs, and aggregation runs in a separate worker. Ten notifications for a 10,000-route source result in one aggregate refresh. |
| 8. Stale ownership completion erases new workspaces | Verified | Completion applies the ownership delta to the current payload. Concurrent workspace addition and protected orphan removal tests pass. |
| 9. Head rebuilding strands additions | Verified | Atomic Lua repair is shared by proxy and scrape queues. Concurrent enqueue and preservation of other shard heads are tested. |
| 10. Checks outlive or lose their lease | Partial | Between-attempt renewal and ordinary stale-completion tests pass. Independent bulk-requeue and legacy-import checks fail as described above. |
| 11. Reputation refresh has a fixed ceiling and volatile backlog | Verified | Versioned requests persist in the statistics transaction. Workers drain continuously; concurrent claims, expired claims, and new events during completion pass PostgreSQL tests. |

## Validation completed

The following pass against the reviewed change set:

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `go build ./cmd/magpie`
- PostgreSQL 17 regressions and real Redis 7 statistics recovery
- Compose configuration validation using `.env.example`
- Docusaurus production build
- `git diff --check` in backend, deployment, and documentation repositories

The full normal and race suites used disposable services through
`MAGPIE_TEST_POSTGRES_DSN` and `MAGPIE_TEST_REDIS_URL`, with `REDIS_URL` and
`redisUrl` unset so test fixtures can override connection failures themselves.
No existing deployment database or Redis was used for these regressions.

Current plaintext queue tests pass with no cipher available. A loaded-script
normal cycle uses `EVALSHA`, interval `GET`, and completion `EVALSHA`, and leaves
the payload unchanged. The deployment and documentation repositories include
the migration requirement, new worker settings, and storage tradeoffs.

The old/new throughput comparison in the implementation report was not
repeated in this verification. Its approximately 20% concurrent queue overhead
remains an implementation measurement. These checks do not establish
tens-of-millions production capacity. A representative sustained workload and
soak test remain necessary, especially for the permanent event ledger and
source aggregation backlog.

## Independent reproduction files

The overlay and tests are retained under
`/tmp/magpie-backend-verify-20261004` for follow-up work:

- [Overlay mapping](/tmp/magpie-backend-verify-20261004/overlay.json)
- [Real Redis queue tests](/tmp/magpie-backend-verify-20261004/queue_verify_test.go)
- [Checker retry test](/tmp/magpie-backend-verify-20261004/checker_verify_test.go)
- [Multiple-page recovery test](/tmp/magpie-backend-verify-20261004/runtime_verify_test.go)
- [Cancellation test](/tmp/magpie-backend-verify-20261004/runtime_shutdown_verify_test.go)

Run from `magpie-backend` with disposable services configured. The queue tests
require `MAGPIE_VERIFY_REDIS_URL` pointing to a dedicated empty Redis database;
their fixture flushes only that database. Recovery tests use the usual
`MAGPIE_TEST_POSTGRES_DSN` and `MAGPIE_TEST_REDIS_URL` variables.

```sh
go test -overlay /tmp/magpie-backend-verify-20261004/overlay.json \
  ./internal/jobs/queue/proxy ./internal/jobs/checker ./internal/jobs/runtime \
  -run '^TestVerify' -count=1 -v
```

The two queue tests, checker retry test, and acknowledgement cancellation test
are expected to fail on this change set. Multiple-page recovery passes.

Full validation and reproduction logs are retained in the same temporary
directory. The disposable containers used for verification were stopped after
the checks completed.
