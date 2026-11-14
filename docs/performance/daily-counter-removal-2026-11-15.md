# Removing the per-route daily check counters from ingestion, 15 November 2026

Step 2b of [ADR 0017](../adr/0017-serve-dashboards-from-shared-generation-stamped-snapshots.md)
removes the `proxy_daily_checks` upsert from every statistics ingestion
transaction, together with daily-counter compaction and orphan cleanup.

## Operation accounting

| Path | Before | After |
| --- | --- | --- |
| Per proxy check (checker workers) | No daily-counter work | Unchanged: 0 cryptographic operations, 0 JSON encodes, 0 Redis commands, 0 database queries added |
| Per accepted statistics batch | One `INSERT … ON CONFLICT` into `proxy_daily_checks`, one row per distinct route and UTC day | Removed; every other statement keeps its order |
| Statistics retention pass | Daily compaction with its backfill | Removed |
| Orphan cleanup pass | Three counter-table deletion loops | Removed |

## SQL-log comparison

`make sql-log` was run on step 2a and on step 2b on the same day. Both digests
have 8,087 transactions and 554 distinct sequences; statements fell from 12,426
to 12,369.

- The 19 ingestion transaction sequences are identical apart from the removed
  daily upsert.
- The development seed transaction writes monthly usage rows instead of daily
  rows and backfill markers. That is intended.
- The remaining differences are test-side reads that replaced daily-counter
  assertions.

## Ingestion cost at one million routes

`TestStatisticsTransactionCostPopulatedPostgres` ran with
`MAGPIE_TEST_STATISTICS_LOCK_ROUTES=1000000`: eight owners, eight million
ownership and latest rows, partitioned history. It ran on step 2a and then on
step 2b, back to back on one quiet host (20 CPUs, 38 GiB). Each run used a fresh
PostgreSQL 17 container limited to 4 CPUs and 4 GiB of memory.

Medians of three trials, one worker, 5,000 physical events per batch:

| Order, events per transaction | 2a ms | 2b ms | 2a WAL MB | 2b WAL MB | Statements |
| --- | ---: | ---: | ---: | ---: | --- |
| usage before latest, 5,000 | 8,498 | 7,905 | 578 | 498 | 33 → 32 |
| usage at tail, 5,000 | 8,846 | 6,939 | 590 | 449 | 33 → 32 |
| usage at tail, 625 | 5,536 | 5,656 | 606 | 233 | 88 → 80 |

The four-worker modes run once each under a shared 30-second deadline. On 2a,
the 625-event four-worker mode missed that deadline and failed the test. On 2b
it finished in 21.9 seconds and the test passed. The other four-worker modes
reached the deadline on both revisions.

## Limits

- These are local measurements on a developer host, not the matched production
  hardware the release gate requires.
- Three trials per single-worker mode and one per four-worker mode cannot
  establish an improvement. They show no regression, and the statement count
  falls by exactly the removed upsert.
- An earlier pair of runs, taken while other test suites shared the host, varied
  by up to 25% in both directions.

Reproduce with a disposable PostgreSQL:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=55432 user=magpie_ci password=magpie_ci dbname=magpie_ci sslmode=disable' \
MAGPIE_TEST_STATISTICS_LOCK_ROUTES=1000000 \
  go test ./internal/database -run '^TestStatisticsTransactionCostPopulatedPostgres$' -count=1 -v -timeout=60m
```
