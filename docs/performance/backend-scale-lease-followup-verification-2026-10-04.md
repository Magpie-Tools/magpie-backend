# Lease follow-up verification

Reviewed on 2026-10-04 against the current uncommitted changes over backend
commit `9c3e77e3ac9dfdf8319e08eb2cfcbfc5618fcc43`.

The three issues reported in the previous verification are fixed. Their
independent reproductions now pass. One additional legacy hash migration path
still permits duplicate workers for a route, so lease ownership is not yet
preserved across every supported compatibility path.

## Confirmed fixes

| Previously reported issue | Verification |
| --- | --- |
| Bulk requeue overwrites an active lease | The original real Redis probe now times out on its attempted second dequeue. Bulk scheduling compares and updates the score atomically, including leases acquired after the member listing. |
| Import moves an active route out of its legacy shard | The original real Redis probe now preserves the existing lease. The production regressions also cover current, legacy, and other configured shards, ownership retention after completion, and expired-lease recovery. |
| Cancellation traps statistics acknowledgement in a retry loop | The original PostgreSQL/Redis cancellation probe now stops. Production regressions recover one, 37, and 5,001 pending events without double-counting committed history or daily checks. |

The checker probe now requires its attempted second dequeue to time out and
the original worker to retain all three attempts. The earlier probe assumed
bulk scheduling had revoked the first lease. Its setup remains the same; the
new assertion verifies exclusivity under the corrected preservation behavior.

Independent multiple-page recovery still passes with 5,001 and 10,037 pending
events. The shutdown fix propagates cancellation through persistence and
checks cancellation before retrying, rather than discarding pending stream
events.

## Remaining issue: legacy rekey can overwrite another worker's lease

Sources:
[current payload lookup](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:505),
[unconditional migration payload write](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:525),
[canonical scheduling overwrite](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:532).

This requires an old-format member under its previous hash and a current-format
member under its current hash to coexist for the same route. It differs from
the import race that was just fixed, which uses the same member hash in two
shards.

When the old-format member becomes due, `GetNextProxyContext` recomputes its
current hash and calls `migrateDequeuedProxyMember`. If that current hash is
already leased, migration nevertheless writes the payload and uses `ZADD GT`
to install its own later expiry. It then returns a second worker for the same
route. `GT` compares expiry values; it does not preserve the existing owner.

The independent real Redis test seeded both supported payload formats for
route ID 1. The current member was due first; the old member became due 100 ms
later. The first worker was still active when the second dequeue migrated the
old member:

```text
same route ID=1/1 same canonical hash=true
canonical score overwritten=true; old worker renewal result=<nil>
legacy hash migration leased an already active route
```

The original worker's renewal fast path still returns success while its
remembered expiry is more than two minutes away. Both workers can continue
checking until a later ownership check rejects the older completion. Completion
fencing cannot prevent requests that have already happened.

The failure reproduced once normally and in all three repetitions with the
race detector enabled. No data race was reported; the failure is queue
ownership behavior. The migration's `GT` overwrite also exists in the baseline,
so this is a newly identified compatibility issue rather than a new regression.

Startup bootstrap publishes current hashes through `AddToQueue`; that operation
cannot identify a previous hash from its different member key. Stopping old
checker processes does not by itself reconcile old and current aliases already
stored in Redis.

Migration must atomically detect and preserve an existing current lease,
merge ownership against the current payload, and consume the legacy alias
without returning a second check. This belongs to the exceptional compatibility
path. The ordinary plaintext checker should retain its current operation budget.

## Validation

All existing checks pass on the reviewed changes:

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `go build ./cmd/magpie`
- PostgreSQL 17 scaling and recovery regressions
- Real Redis 7 queue and recovery regressions using separate disposable databases
- Compose validation with `.env.example`
- Docusaurus production build
- `git diff --check` in backend, deployment, and documentation repositories

The normal plaintext command-budget regression passes against real Redis:
dequeue `EVALSHA`, interval `GET`, and completion `EVALSHA`. Hash reuse without
a cipher and unchanged payload contents also pass. The lease marker adds no
cryptography, JSON encoding, payload writes, database queries, or client Redis
commands to that normal cycle.

I did not repeat the old/new throughput comparison in the implementation report.
These checks do not establish production capacity or replace a sustained soak.
The documented migration and requirement to stop previous checker instances
remain necessary.

No runtime code was edited during verification. The earlier reports remain
unchanged. Independent test sources and logs are retained in
`/tmp/magpie-backend-verify-followup-20261004`:

- [Passing original probes](/tmp/magpie-backend-verify-followup-20261004/independent.log)
- [New migration regression](/tmp/magpie-backend-verify-followup-20261004/queue_alias_verify_test.go)
- [Normal reproduction log](/tmp/magpie-backend-verify-followup-20261004/legacy-rekey-independent.log)
- [Race-enabled reproduction log](/tmp/magpie-backend-verify-followup-20261004/legacy-rekey-race-independent.log)
- [Overlay mapping](/tmp/magpie-backend-verify-followup-20261004/overlay.json)

To reproduce, configure `MAGPIE_VERIFY_REDIS_URL` to an empty disposable Redis
database and run from `magpie-backend`. The test fixture flushes that database.

```sh
go test -race \
  -overlay /tmp/magpie-backend-verify-followup-20261004/overlay.json \
  ./internal/jobs/queue/proxy -run '^TestVerifyLegacyRekey' -count=3 -v
```

This regression is expected to fail on the reviewed changes. The disposable
verification containers were stopped after validation.
