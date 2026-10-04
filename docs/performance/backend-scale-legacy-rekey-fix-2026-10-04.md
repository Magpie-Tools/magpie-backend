# Legacy hash migration lease fix

This change addresses the remaining migration failure in
[the lease verification](backend-scale-lease-followup-verification-2026-10-04.md).
The independent real Redis reproduction failed in all three race-enabled runs
before the change and passes in all three runs afterward.

## Ownership during migration

Old and current hashes can coexist in Redis after bootstrap. Their distinct
members can both become due for the same route. The previous migration read
the current payload in Go and then used `ZADD GT` to install its own expiry.
That comparison could overwrite an active lease or return a second worker
whose lease did not match the existing score.

Migration now uses one Lua script to validate the popped alias's lease and
payload, merge workspace owners from the live current payload, remove the old
alias, and update affected shard heads. It searches the supported shards for
an active current lease. When it finds one, that lease and its shard remain
unchanged. The dequeuer consumes the alias and continues looking for work.
It returns no second check for that route.

When the current route is unleased or its lease has expired, migration acquires
one lease under the current hash. Concurrent aliases converge on that lease.
The current payload's route ID stays authoritative, and existing credentials
remain intact unless their queue representation needs conversion. Workspace
owners added during migration survive both the merge and later completion.

A reclaimed or deleted source cannot be migrated by its former candidate.
When an import changes the source payload while that candidate still owns its
score, migration releases only that score and lets dequeue decode the updated
payload. Format conversion under an unchanged hash uses the same ownership
and payload checks. Migration returns the exact payload snapshot written with
its lease, so a later import cannot become the worker's starting snapshot.

## Regression coverage

Permanent tests cover an active current lease in current, legacy, and other
configured shards; a current claim and owner import during migration
preparation; concurrent plaintext and encrypted aliases; source reclamation,
deletion, and owner updates; and recovery of an expired current lease.
The original worker can complete after alias consumption, and its ownership
delta retains owners introduced by the alias.

Use `MAGPIE_TEST_QUEUE_REDIS_URL` for a separate disposable Redis database.
The queue fixture flushes it. The independent reproduction uses
`MAGPIE_VERIFY_REDIS_URL` and remains unchanged under
`/tmp/magpie-backend-verify-followup-20261004`.

## Operation budget and deployment

The ordinary plaintext cycle still reuses the stored hash and issues dequeue
`EVALSHA`, interval `GET`, and completion `EVALSHA`. This fix adds zero
cryptographic operations, JSON encodes, payload writes, Redis commands, or
database queries to that cycle. The no-cipher and unchanged-payload regressions
remain in the suite.

Only legacy rekey or payload conversion invokes the migration script. Legacy
fingerprinting and optional secret conversion remain confined to that path.
Migration uses one warm `EVALSHA`, replacing the separate current-payload
lookup, transaction pipeline, and post-write snapshot lookup. A current-format
collision that needs an ownership merge adds one Lua JSON encode after the Go
encode of the normalized source. That encode is necessary to merge against
the payload and lease observed within the same atomic operation. Migration
performs no database credential loads.

No additional PostgreSQL migration or queue flush is required for this fix.
Legacy plaintext and encrypted readers remain supported. Keep
`PROXY_QUEUE_ENCRYPT_CREDENTIALS=false` as the default and preserve
`PROXY_ENCRYPTION_KEY`. Stop previous checker implementations before starting
the updated workers, and keep the shard count consistent across instances.
Aliases already stored in Redis reconcile as workers encounter them; an active
current route retains its owner during that reconciliation.

## Local measurements

Before and after binaries ran sequentially against a disposable Redis 7
instance on the same i7-12700H host with `GOMAXPROCS=4`. Each result below is
the median of three samples. Ordinary cycles used 20,000 iterations per
sample; legacy rekey used 2,000. The rekey fixture starts with eight owners
under the current hash and adds a ninth owner through an old alias. Fixture
setup and route hashing occur outside the timer.

| Queue operation | Before, microseconds/op | After, microseconds/op | Go allocations before / after |
| --- | ---: | ---: | ---: |
| Ordinary serial cycle | 133.807 | 133.141 | 53 / 53 |
| Ordinary cycle with 32 workers | 38.776 | 42.835 | 53 / 53 |
| Legacy rekey and completion | 260.561 | 348.200 | 163 / 135 |

The concurrent ordinary-cycle median increased by 10.5%. Its sample ranges
overlap, at 38.0 to 46.3 microseconds before and 42.6 to 47.5 afterward.
The serial median changed by less than 1%. These samples establish neither
a throughput improvement nor a production capacity estimate. The command,
encoding, and cryptography regressions establish the unchanged normal
operation budget.

The exceptional rekey median increased by 33.6%. Its Go allocation volume
fell from about 32.1 KB to 12.9 KB per operation. The migration now checks
source ownership, searches configured shards for a current lease, and merges
owners inside Redis before writing. That measured cost buys atomic lease and
payload handling for stored aliases, including the additional Lua JSON encode
when owners change. The ordinary current-format path does not run this script.

Raw measurements are in `/tmp/magpie-legacy-rekey-cycle-before.log`,
`/tmp/magpie-legacy-rekey-cycle-after.log`,
`/tmp/magpie-legacy-rekey-migration-before.log`, and
`/tmp/magpie-legacy-rekey-migration-after.log`. The identical-workload baseline
uses the pre-fix runtime copied to `/tmp/magpie-legacy-rekey-before-20261004`.

## Validation

The following checks pass:

- The independent legacy rekey reproduction, three runs with the race detector
  against real Redis. All three runs failed against the pre-fix implementation.
- Permanent migration regressions and the normal plaintext command-budget
  regression, three runs with the race detector against real Redis.
- `go test ./... -count=1`.
- `go test -race ./... -count=1`, with the disposable PostgreSQL and Redis
  fixtures enabled for the database, statistics, and queue regressions.
- `go vet ./...` and `go build ./cmd/magpie`.
- `docker compose --env-file .env.example config --quiet` in `magpie`.
- `npm run build` in `magpie-docs`.

The original review and verification reports remain unchanged.
Production-scale capacity and a multi-day soak remain unverified.
