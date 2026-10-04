# Independent verification of legacy rekey migration

Verified on 2026-10-04 against the current working tree. Backend HEAD remains
`9c3e77e3ac9dfdf8319e08eb2cfcbfc5618fcc43`; the implementation is uncommitted.

The legacy lease failure described in
[the previous verification](backend-scale-lease-followup-verification-2026-10-04.md)
is fixed in the cases reviewed. The original real Redis reproduction now
passes three runs with the race detector. A canonical worker remains the sole
worker for its route when a later legacy alias becomes due.

## Migration behavior

The migration script validates the alias's processing score and source payload
snapshot before changing the queue. It searches the supported shards for an
active canonical lease, merges owners from the live canonical payload, removes
the alias, and repairs affected queue heads in the same Redis operation.
When a canonical worker already owns the route, migration preserves that
worker's score and shard, and dequeue continues without returning another
worker. Concurrent aliases acquire one canonical lease when no active lease
exists.

The script returns the payload snapshot belonging to the lease it acquires.
An additional independent probe imports another workspace immediately after
the migration command succeeds, before dequeue receives the reply. The worker
retains the earlier snapshot, and its subsequent ownership-changing completion
preserves that newly imported workspace. This probe also uses different IDs
for the alias and canonical payload and verifies that the canonical ID and
credentials survive migration. It passes three race-detector runs against
real Redis.

The permanent migration regressions pass against real Redis for active leases
in current, legacy, and other configured shards; canonical claims and imports
during migration preparation; concurrent plaintext and encrypted aliases;
changed, deleted, or reclaimed source candidates; and expired canonical leases.

Independent checks from the earlier review also pass: imports and bulk requeue
preserve active leases, checker retries keep their exclusive worker, statistics
shutdown finishes after cancellation during acknowledgement, and pending
recovery processes 5,001 and 10,037 events without duplicate history rows.

## Validation

All commands below were run independently. PostgreSQL 17 and Redis 7 ran in
disposable containers bound to random localhost ports. Normal tests, race
tests, and independent probes used separate Redis databases. Existing user
services were untouched; the verification containers were removed afterward.

| Check | Result |
| --- | --- |
| Original legacy rekey reproduction, `-race -count=3` | Pass |
| Independent atomic snapshot and canonical ID probe, `-race -count=3` | Pass |
| Earlier independent checker, queue, and statistics probes, `-race` | Pass |
| `go test ./... -count=1`, with real PostgreSQL and Redis fixtures | Pass |
| `go test -race ./... -count=1`, with real PostgreSQL and Redis fixtures | Pass |
| `go vet ./...` | Pass |
| `go build ./cmd/magpie` | Pass |
| Compose configuration with `.env.example` | Pass |
| Documentation `npm run build` | Pass |
| `git diff --check` in backend, deployment, and docs repositories | Pass |

Independent fixture sources, overlay configuration, and logs are saved under
`/tmp/magpie-backend-verify-rekey-20261004`. The reproduction was reused from
the earlier review; the new snapshot probe also lives outside the repository.
No runtime code was changed during this verification.

## Performance and deployment limits

The current plaintext path returns before the migration script. This follow-up
adds zero cryptographic operations, JSON encodes, Redis client commands, or
database queries to a normal proxy check. The command-budget regression passes
with three warm client commands for dequeue and completion, no encryption key,
an unchanged stored route hash, and an unchanged payload. Exceptional migration
does inspect configured shards and may encode merged ownership inside Redis.

The raw benchmark logs referenced in
[the implementation report](backend-scale-legacy-rekey-fix-2026-10-04.md)
support its reported medians and approximately 34% higher legacy migration
cost. These benchmark samples were inspected, rather than rerun in this
verification. They do not establish production capacity. Tens of millions of
routes and thousands of users still require representative load and soak
validation, including backlog age, recovery, database write rate, and storage
growth.

Stop previous checker instances before starting the updated implementation.
Run `--migrate-only` for the earlier event-ledger and reputation-queue schema
changes. Legacy alias reconciliation adds no further database migration or
queue flush. Keep the encryption key stable and the shard count consistent
across instances. Existing duplicate database rows remain, and the durable
event ledger continues to require storage beyond history retention.
