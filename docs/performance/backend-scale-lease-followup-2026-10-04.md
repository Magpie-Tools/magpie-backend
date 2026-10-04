# Lease and statistics shutdown follow-up

This follow-up addresses the three failures in
[the independent verification](backend-scale-verification-2026-10-04.md).
The original queue and shutdown reproductions failed on a saved copy of the
previous implementation, and pass after these changes. The checker regression
now asserts that bulk scheduling cannot grant a second lease while the original
worker retries. Bulk requeue preserves ownership, so the original worker keeps
its retry budget.

The subsequent verification found an alias migration race when old and current
hashes coexist for one route. Its fix and regressions are recorded in
[the legacy rekey follow-up](backend-scale-legacy-rekey-fix-2026-10-04.md).

## Queue ownership

Scheduled scores are integer Unix milliseconds. Leased scores use the expiry
in milliseconds plus `0.5`, which Redis's double precision represents exactly
at current timestamps. Workers retain the integer expiry and the actual owned
shard. Dequeue, renewal, completion, and removal use the same marker. This
distinguishes leases without another Redis key, command, or payload field.

Bulk requeue checks the current score and writes the new due time in one Lua
script per batch of at most 500 members. A worker that claims after the member
list was read is still protected. Marked leases, including expired ones, keep
their score. Expired leases remain due and can be reclaimed normally; stale
workers then fail renewal, completion, or removal. `proxy_count` in
`POST /api/global/proxies/requeue` counts only the members actually rescheduled.

Imports update the payload and scheduling atomically. They inspect the
destination, legacy queue, and configured shards, keep a leased member in its
owned shard, and remove duplicate scheduling entries from the other supported
shards. The active worker can finish normally and migrate its scheduling to
the current shard. Imported workspace ownership survives that completion.
The shard count must remain consistent across instances.

## Statistics shutdown

Stream workers check cancellation before persistence and acknowledgement
retries. Retry waits wake on cancellation, and database persistence uses the
worker context with the existing 30-second insert timeout. On cancellation,
unacknowledged entries remain pending for recovery. The event ledger prevents
already committed entries from incrementing history or daily usage again.
There is no acknowledgement loop with a cancelled context or unbounded
shutdown flush. Another consumer can recover pending entries once they have
been idle for at least one minute.

The volatile in-memory fallback also checks cancellation before retries and
makes one final persistence attempt with the 30-second insert timeout. It
cannot provide durable recovery if that attempt fails.

## Regression coverage

- Bulk requeue preserves active leases, including a claim between listing and
  the scheduling write. Completed work can still be bulk rescheduled.
- Imports preserve leases in current, legacy, and different configured shards,
  retain newly added workspaces, and leave expired leases recoverable.
- A retrying checker remains the only worker on its route during bulk requeue.
- Cancellation during acknowledgement leaves one, 37, and 5,001 events pending.
  A restarted worker recovers them with exact history, event ledger, and daily
  check counts. The large case uses real PostgreSQL and Redis.
- Cancelled persistence does not acknowledge uncommitted entries. A failed
  in-memory batch cannot trap its worker in a retry loop during shutdown.
- Current plaintext payloads reuse the stored hash without a cipher, and
  ordinary completion leaves the payload unchanged.

Use `MAGPIE_TEST_QUEUE_REDIS_URL` for the queue regression fixture. It flushes
its database and requires an empty disposable database separate from
`MAGPIE_TEST_REDIS_URL`, which supplies stream tests. The latter use isolated
stream keys. Set `MAGPIE_TEST_POSTGRES_DSN` for the PostgreSQL recovery cases.
Keep `REDIS_URL` and `redisUrl` unset during full test runs so failure fixtures
can override their connections.

## Operation budget and rollout

This follow-up adds zero cryptographic operations, JSON encodes, payload
writes, client Redis commands, internal Redis commands, or database queries
to an ordinary plaintext check. A warm cycle still has dequeue `EVALSHA`,
interval `GET`, and completion `EVALSHA`. Marker arithmetic uses the existing
sorted-set reads and writes. A long check still renews only when its remaining
lease is under two minutes.

Imports and bulk scheduling are outside that cycle. Imports now use one atomic
`EVAL` per route and inspect the configured shards. Bulk scheduling adds a
`ZSCORE` before each candidate update inside its batch script. These reads
prevent either administrative operation from granting concurrent ownership.

Local Redis 7 measurements compare the saved pre-follow-up implementation
with this version using three 20,000-cycle samples and `GOMAXPROCS=4`.
Both versions use 53 allocations per cycle. Concurrent samples use 32 workers.

| Queue cycle | Before follow-up median | After follow-up median |
| --- | ---: | ---: |
| One worker | 141.1 µs | 131.3 µs |
| 32 workers | 43.6 µs | 41.8 µs |

Sample ranges overlap. These component measurements do not establish an
increase in deployment throughput or remove the need for a production soak.

Stop previous checker instances before starting this queue implementation.
Old workers do not understand marked lease scores and cannot safely share the
queue with new workers. Existing integer due scores, legacy plaintext and
encrypted payloads, and the default plaintext credential policy remain
supported. Keep `PROXY_ENCRYPTION_KEY` stable.

The existing migration requirements and storage limits in
[the scale fix report](backend-scale-fixes-2026-10-04.md) still apply.
Production capacity and a multi-day soak remain unverified.

## Completed validation

- `go test ./... -count=1`
- `go test -race ./... -count=1` with disposable PostgreSQL 17 and Redis 7,
  including the million-route and 65,536-candidate scaling regressions
- `go vet ./...`
- `go build ./cmd/magpie`
- The original independent bulk-requeue, legacy-import, and acknowledgement
  cancellation probes, plus the permanent checker exclusivity regression
- Real Redis/PostgreSQL cancellation and replay regressions at 1, 37, and
  5,001 events, and atomic queue regressions against real Redis
- Compose validation with `.env.example` and the Docusaurus production build
- `git diff --check` in the backend, deployment, and documentation repositories

The review and verification reports are preserved unchanged. Temporary test
services were isolated from the running Magpie installation.
