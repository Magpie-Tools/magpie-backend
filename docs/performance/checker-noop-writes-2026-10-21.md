# Skip unchanged failure counters and proxy metadata

The release preflight still overloaded PostgreSQL after the projection index
cleanup. Two existing statements generated new row versions even when their
stored values were already correct. A successful checker cycle reset every
owner's failure counter from zero to zero, and metadata enrichment rewrote
unchanged country/type values, including repeated `N/A` results.

The failure reset now adds `consecutive_failures <> 0` to its existing statement.
The metadata update adds a row-value `IS DISTINCT FROM` predicate for country and
type. Batch sizes, statement counts, timeouts and transaction boundaries remain
the same. Nonzero streaks still reset, and real metadata changes still persist.
Enrichment remains eligible to replace unknown values when GeoLite data changes.

PostgreSQL failure resets use raw SQL and have never advanced
`ManagedProxy.UpdatedAt`. The guarded statement preserves that behavior for true
resets and leaves every other field unchanged. The SQLite fallback now leaves
the mutation timestamp unchanged on zero streaks; true resets retain its existing
GORM timestamp behavior. Health freshness continues to use persisted checker
evidence, rather than the ownership mutation timestamp.

## Measured write costs

The isolated fixture ran PostgreSQL 17.11 with two CPU slots, a 2 GiB memory
limit, a 4 GiB memory-plus-swap limit, 128 MiB shared buffers and 4 MiB work
memory. Tests used Go 1.27.2 and `GOMAXPROCS=4`. It contained 10,000 shared routes
and 80,000 ownership rows for failure resets, and 10,000 proxy rows with their
query indexes for metadata. Candidates were spread across the inventory.

Each comparison used three `EXPLAIN ANALYZE` samples with buffers and WAL. The
original statement performs the previous unconditional writes; the guarded
statement uses the final SQL. Functional audit triggers were removed before
timing so their inserts do not inflate the measured WAL.

| Operation | Existing target rows | Original median SQL time | Guarded median SQL time | Original mean WAL records | Guarded mean WAL records |
| --- | ---: | ---: | ---: | ---: | ---: |
| Already-zero failure reset | 200 ownerships | 1.739 ms | 0.535 ms | 1,046.3 | 1.7 |
| Equal country/type update | 512 proxies | 12.711 ms | 1.859 ms | 4,570.3 | 2.7 |

WAL records fell 99.84% and 99.94% respectively. The first guarded samples
performed a small amount of cleanup of row versions left by the original
statements; subsequent samples generated no WAL. Separate audit assertions prove
that equal-value requests perform zero row updates. These are targeted write-cost
measurements, not a concurrent million-route capacity result.

`TestFailureResetAvoidsZeroWritesAndLocksPostgres` holds an already-zero ownership
row locked and verifies that a full 200-pair reset completes without waiting.
Only one deliberately nonzero streak changes. Repeating the reset causes no
further updates, preserves unrelated failures and leaves credentials, lifecycle
and mutation timestamps intact. The SQLite regression and existing threshold,
pause/delete and successful-reset tests pass as well.

`TestMetadataWritesOnlyChangedValuesPostgres` similarly holds an unchanged proxy
row locked. Only the two candidates with a country-only or type-only change
update; repeating the batch changes nothing. Route host, IP projection, port and
stored route hashes remain intact.

## Billing and ingestion locks

The current checker accounting path aggregates workspace/month deltas, sorts
them and atomically adds them to `workspace_usage_periods`. It does not acquire
the subscription lock or count ownership inventory. Billing source is unchanged.

`TestCheckUsageDoesNotWaitForSubscriptionLockPostgres` deliberately holds the
subscription row `FOR UPDATE`. Check accounting still commits in 20.3 ms in the
normal test, counts an owner once despite duplicate attribution, records three
attempts and retains the existing active/peak capacity values. Its stored
capacity is set to one million to detect recounts; the functional fixture has one
physical route. Replay, rollback, concurrent replay and monthly-capacity carry
regressions also pass.

Imports still use the existing subscription lock and exact active-route recount.
A new-association-only shortcut would also need to repair a missing monthly
usage row. This change leaves that behavior and capacity enforcement intact.

## Hot-path operation counts and validation

Added work per proxy check is zero cryptographic operations, route-fingerprint
calculations, JSON encodes, Redis commands, database credential loads and database
statements. A reset chunk still sends one update; a metadata batch still sends
one update. The predicates add comparisons inside those statements and avoid
unnecessary row versions, index maintenance and row locks.

Plaintext queue defaults, stored-hash reuse, legacy queue readers and scheduling
requeues are unchanged. No encryption, decryption, nonce generation or payload
rewrite was added to the checker loop.

Normal targeted tests and race tests passed with `MAGPIE_TEST_POSTGRES_DSN`
pointing to a private disposable database. The PostgreSQL tests run whenever that
variable is supplied; they have no additional opt-in switch. Full combined
`go test ./... -count=1`, `go test -race ./... -count=1`, `go vet ./...` and
`go build ./cmd/magpie` remain part of the sequential release validation driver.

Exact commands, exit codes, all twelve analyzed plans, fixture limits, source
hashes and machine-readable results are retained in
`.codex-tmp/release-remediation-20261020/write-guards-20261021/` in the workspace.
