# Verification log of the architecture v4 implementation

This is the durable record of what was checked for each commit of the staged
extraction described in
[ADR 0015](../adr/0015-extract-feature-packages-behind-architecture-tests.md):
the per-commit suites, the SQL-log comparisons, the onboarding checkpoints,
the `readmodel` pilot's gates, the hot-path ownership changes, the opt-in scale
regressions and the items still open. Every number here was measured;
anything not measured is listed as such.

## How each commit was checked

Each commit was exported with `git archive` into an empty directory, so no
uncommitted file could influence the result, and checked there with:

| Check | Command |
| --- | --- |
| Build | `go build -o /dev/null ./cmd/magpie` |
| Vet | `go vet ./...` |
| Unit | `go test ./... -count=1` |
| Race | `go test -race ./... -count=1` |
| Integration | `go test ./... -count=1` with `MAGPIE_TEST_POSTGRES_DSN`, `MAGPIE_TEST_REDIS_URL` and `MAGPIE_TEST_QUEUE_REDIS_URL` set |

The integration services were disposable PostgreSQL 17 and Redis 7
containers, started for this work under unique names on free local ports and
never shared with any existing container. The per-commit race run used SQLite
and miniredis; the race suite against the real services (CI's
`go test -race` step) ran on `origin/main` before the work started and on the
commits named in the notes below.

Hardware for every run, including the benchmarks: one Linux amd64 machine
with 20 logical CPUs and 38 GB of memory, Go 1.27.4.

## Per-commit results

| Commit | Subject | Build | Vet | Unit | Race | Integration | SQL log |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `8294ef3` | Delete deprecated wrappers that have no production callers | pass | pass | pass | pass | pass | - |
| `49c9d69` | Add batching and composition tests for the checker batchers | pass | pass | pass | pass | pass | - |
| `4db35e7` | Add make test-integration with disposable PostgreSQL and Redis | pass | pass | pass | pass | pass | - |
| `c4c4c6d` | Document every package and index the performance notes | pass | pass | pass | pass | pass | - |
| `21c8fca` | Record the onboarding baseline and add the SQL-log comparison | pass | pass | pass | pass | pass | - |
| `22def17` | Add architecture tests and the repository guide | pass | pass | pass | pass | pass | - |
| `b50b951` | Add the rename-only move check | pass | pass | pass | pass | pass | - |
| `d125f37` | Extract platform/pg: SQL helpers and all database connection creation | pass | pass | pass | pass | pass | identical to b50b951 |
| `994dfaa` | movecheck: fold the export case of constant and variable names | pass | pass | pass | pass | pass | - |
| `c6f298b` | Extract platform/schema: ordered schema steps and migrations | pass | pass | pass | pass | pass | identical to b50b951 |
| `c8cc25c` | movecheck: keep a local variable that shares a package's name | pass | pass | pass | pass | pass | - |
| `7edec44` | Extract evidence: current workspace evidence SQL and pool eligibility | pass | pass | pass | pass | pass | identical to b50b951 |
| `163e58f` | Add the shared database test kit | pass | pass | pass | pass | pass | new reference: `b50b951` plus the renamed test schemas |
| `e9d6dca` | Extract mail: the email outbox and its delivery routines | pass | pass | pass | pass | pass | identical to 163e58f |
| `6034675` | Move the shared pool handle to platform/pg | pass | pass | pass | pass | pass | identical to 163e58f |
| `f38d614` | Extract alerts: rules and destinations | pass | pass | pass | pass | pass | identical to 163e58f |
| `7909bbb` | archtest: actually check the package of documented go test snippets | pass | pass | pass | pass | pass | - |
| `ea56f23` | Extract alerts: evaluation, transitions and delivery | pass | pass | pass | pass | pass | identical to 163e58f |
| `7a51ea4` | Record the onboarding checkpoint after alerts | pass | pass | pass | pass | pass | - |
| `2d1566a` | Extract account: user lookup, passwords, reset tokens, notification reads | pass | pass | pass | pass | pass | identical to 163e58f |
| `ebd31ec` | Move AbuseIPDB persistence into abuseipdb | pass | pass | pass | pass | pass | identical to 163e58f |
| `e57b651` | Move GeoLite persistence and the geo refresh into geolite | pass | pass | pass | pass | pass | identical to e9d6dca |
| `280996e` | Move IP blacklist persistence into blacklist | pass | pass | pass | pass | pass | identical to e9d6dca |
| `4282897` | Extract judge persistence into judgestore | pass | pass | pass | pass | pass | identical to e9d6dca |
| `c226b99` | Record source tag transactions in the SQL log | pass | pass | pass | pass | pass | stable (two runs) |
| `13fd97d` | Extract the scraper store: source CRUD and scheduling | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `8c1c2ae` | Extract the development seed into devseed | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `d9a3e5b` | Move NormalizeEmail to domain and IsUniqueConstraintError to platform/pg | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `3ac89be` | Extract workspace: memberships, access, invitations and capacity policy | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `f5baa1f` | Record the onboarding checkpoint after the peripheral features | pass | pass | pass | pass | pass | - |
| `e99019b` | Give the checker batchers an explicit owner and stop order | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `1ccd4f7` | Record the verification of the architecture v4 implementation | pass | pass | pass | pass | pass | - |
| `837a705` | Move CountActiveInstances to instanceid | pass | pass | pass | pass | pass | - |
| `d24768a` | Move the GraphQL schema's helpers into feature files | pass | pass | pass | pass | pass | - |
| `189f40b` | Split the GraphQL schema into per-feature constructors | pass | pass | pass | pass | pass | - |
| `e24dc03` | Give the check usage meter and its snapshot one owner, the Pacer | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `951ecb1` | Move the read-model schema DDL to platform/schema | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `eeb28a7` | Move DeleteChunkSize to platform/pg | pass | pass | pass | pass | pass | identical to c226b99-run1 |
| `7c8ee7a` | Build the read-model test fixtures without core ingestion | pass | pass | pass | pass | pass | differs from c226b99-run1 only in the rebuilt fixture; see below |
| `6e8c036` | Give core tests a public seam into the read model | pass | pass | pass | pass | pass | identical to 7c8ee7a |
| `901b281` | Add the comparison harness for the readmodel pilot | pass | pass | pass | pass | pass | identical to 6e8c036 |
| `0422769` | Add rollback and locking tests for the read model's callers | pass | pass | pass | pass | pass | new reference: identical to 901b281; two runs identical |
| `59aab12` | Extract readmodel: the proxy filter index and scrape-source counts | pass | pass | pass | pass | pass | identical to 0422769-run1 |
| `277ebf1` | onboarding: follow anchors through export-case and method changes | pass | pass | pass | pass | pass | - |

Notes:

- `7edec44`: the first integration run timed out in
  `TestProxyIngestionBatchesPostgres` while a second, unrelated test run was
  using the same PostgreSQL server. Re-run alone on the same commit, it
  passed (the table shows the re-run). After that, foreground runs used a
  second disposable service pair so verification never shared a server.
- The integration runs skip the browser scraper tests, which need a local
  Chromium, and the opt-in scale regressions (`MAGPIE_TEST_*_ROUTES`,
  `MAGPIE_TEST_ALERT_SCALE`). The browser tests were not run; the scale
  regressions ran separately on selected revisions, recorded under
  [Scale regressions](#scale-regressions).
- The race suite against the real services also ran for `e99019b`,
  `e24dc03` and `59aab12`, the commits that change hot-path ownership or
  split the core: pass each time.

## SQL-log comparisons

The procedure is in [sql-log.md](sql-log.md). Digests group statements by
backend process and virtual transaction ID and count each distinct
per-transaction sequence.

| Reference | What it covers | Stability proof |
| --- | --- | --- |
| `3f66f5d` / `b50b951` | The initial test list: 7855 transactions, 541 distinct sequences, 12182 statements | Three runs of `3f66f5d` byte-identical; `b50b951` identical to them |
| `163e58f` | Same list. The test kit renamed the test schemas from `proxy_ingestion_batches_N` to `magpie_test_N`, so the `CREATE SCHEMA` and `DROP SCHEMA` lines differ from `b50b951`; every other line is identical | Bodies identical to `b50b951` after that rename |
| `c226b99` | The list plus the source tag test, added before the scraper store moved: 8087 transactions, 554 distinct sequences, 12474 statements | Two runs of `c226b99` byte-identical |
| `0422769` | Same list after the read-model fixtures stopped calling statistics ingestion (`7c8ee7a`): 8087 transactions, 554 distinct sequences, 12426 statements. The reference for the `readmodel` pilot | Two runs of `0422769` byte-identical |

Every extraction commit's digest is byte-identical to the reference of its
test list. The commits from `e9d6dca` to `ebd31ec` were first compared with
`b50b951` and reported as different; the only difference is the schema name
introduced by `163e58f`, and each of them is byte-identical to `163e58f`.

`7c8ee7a` changes what four listed read-model tests do before they measure:
their fixture writes the workspace's checker key and the latest evidence
row directly instead of loading checker settings and calling
`InsertProxyStatistics`. Its digest differs from `c226b99` in exactly that:
four ingestion transactions (statistics, daily checks, latest evidence,
reputation request) are gone, and four workspace-key updates and four
single-statement evidence inserts appear (12,474 to 12,426 statements). Every
later commit up to the pilot is identical to it, and the pilot `59aab12`,
whose read-model tests now run from `./internal/readmodel`, is byte-identical
to the two-run reference `0422769`.

## Onboarding checkpoints

Measured with `go run ./internal/tools/onboarding`; the procedure and the full
tables are in [README.md](README.md).

| Checkpoint | Commit | Must-read tokens | Change | Search-space tokens |
| --- | --- | ---: | ---: | ---: |
| Baseline | `3f66f5d` | 89,070 | | 902,720 |
| After step 0 | `b50b951` | 98,397 | +9,327 | 899,888 |
| After `alerts` | `7a51ea4` | 95,130 | -3,267 | 491,541 |
| After D5 | `f5baa1f` | 94,791 | -339 | 332,569 |
| Re-run after the `readmodel` pilot | `59aab12` | 96,573 | +1,782 | 349,652 |

The stop rule ("stop extracting when a re-run shows no further drop") did not
fire at the three checkpoints. The D5 drop is small (-0.4%) and comes from one
task, workspace invitations, whose anchors moved; tasks whose anchors stay put
grow with every row added to the `AGENTS.md` package map. As agreed, that
drop counted, and the `readmodel` pilot went ahead.

The re-run after the pilot shows no drop (+1,782, +1.9%), so **the stop rule
fires: no further package is split out of the core.** The rise is spread over
the ownership changes, the pilot's preparation and the pilot itself; the
per-commit numbers are in [README.md](README.md). The first re-run reported
four unresolved anchors: three functions had become `checker.Runtime` methods
and one had changed export case in a move. `277ebf1` makes the resolver
follow them; on `f5baa1f` it reproduces the D5 number exactly.

## The ADR 0015 gates

### D7 `readmodel` pilot: done, all gates met

The read model moved out of the core in `59aab12`, after preparation that
removed the blocker recorded at `1ccd4f7` (21 exports, 17 of them for tests):

| Commit | Preparation |
| --- | --- |
| `951ecb1` | The read-model schema DDL moves to `platform/schema` (`EnsureReadModelSchema`), so tests in any package create the schema without a test-only export. Pure move. |
| `eeb28a7` | `DeleteChunkSize`, the read model's only reference into the rest of the core, moves to `platform/pg`. Pure move. |
| `7c8ee7a` | Test-only. The read-model tests build on the test kit and plain rows instead of core code; the fixture writes the state that checker settings and statistics ingestion leave for the read model directly. Real ingestion keeps its own core tests through `InsertProxyStatistics` (`ingestion_read_model_postgres_test.go`), which fail when ingestion stops queuing its routes. |
| `6e8c036` | Test-only. The three core tests that reached into the read model's state use the public seam instead: `StartReadModelRefreshRoutine` with an ended context drains all pending work. |
| `901b281` | Test-only. The comparison harness, committed before the move. |
| `0422769` | Test-only. Rollback and locking tests for the read model's callers, committed before the move so they hold across it. |

Gates of `59aab12`:

| Gate | Evidence |
| --- | --- |
| Compiler-verified direction | `rules/layers.txt`: `forbid readmodel: core features transport bootstrap auth checker-execution listeners`. `readmodel` imports `domain`, `evidence`, `platform/pg`, `platform/schema` and `support` only. |
| Reviewed public API | 10 functions, none exported for tests. 6 were exported already (`QueueReadModelRefreshForProxyIDs`, `StartReadModelRefreshRoutine`, `RefreshReadModelIndexes`, `RefreshWorkspaceProxyFilterIndexes`, `RefreshWorkspaceScrapeSourceStats`, `RefreshWorkspaceScrapeSourceStatsForWorkspaceSites`); 4 are newly exported for their production callers in the core (`EnsureReadModelBackfill`, `RefreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs`, `RefreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs`, `RefreshWorkspaceScrapeSourceStatsForSites`). For the parent's review. |
| Pure move | `make move-check BASE=0422769`: 144 declarations moved unchanged, none added or removed. One changed declaration the tool cannot normalize: `shutdownProbeMain`, a string constant holding Go source, gains the `readmodel` import and one qualifier. |
| Unchanged SQL and statement counts | SQL log byte-identical to the two-run reference `0422769`; the read model's statement-count tests pass in their new package. |
| Rollback | All six read-model functions the core calls inside its transactions have a test that runs them in a caller's transaction, rolls back, and finds their writes gone and the transaction still open. |
| Locking | Overlapping callers projecting the same 4,000 routes in opposite orders do not deadlock; the asynchronous drain leaves an open caller's row alone, or waits for it when it must write the row and finishes after the commit. The read model's own lock and timeout tests moved with it. |
| Repeated scale comparisons | Below, and the read-model scale regressions under [Scale regressions](#scale-regressions). |
| Onboarding | The re-run after the pilot shows no drop; the stop rule fires (see above). |

**Comparison harness.** `TestReadModelComparisonPostgres` on 20,000 routes
shared by 4 workspaces and 20 sources: 8 rounds of 5,000 statistics through
`InsertProxyStatistics`, each followed by a full drain, then 10 rounds with the
refresh routine running (its 1-second tick bounds lag from below). Base
`0422769`, pilot `59aab12`, alternating on one otherwise idle disposable
PostgreSQL 17 (`shared_buffers=512MB`, `fsync=off`). The rule, fixed before
the first run: a metric regresses when the pilot's median lies outside the
base's range in the worse direction.

| Batch | Runs | Flagged |
| --- | --- | --- |
| 1 | 6 base, 6 pilot | ingest p95 (912 ms against a base range of 844-892 ms), projection Go bytes per statistic (+4.2 bytes, +0.07%) |
| 2 | 6 base, 6 pilot, 6 base again as an A/A control | ingest WAL (+3.8%) and projection WAL (+2.2%); the A/A control's ingest WAL differed from the base's by 6.5% |
| 1 and 2 together | 12 base, 12 pilot | none |
| 3, without checkpoints or autovacuum during the runs | 6 base, 6 pilot, 6 A/A control | none, in A/B and in A/A |

Batch 3 isolates WAL from checkpoint timing and full-page images:

| Metric (batch 3) | Base median | Pilot median | Change | Base range | Pilot range |
| --- | ---: | ---: | ---: | --- | --- |
| Statistics per second | 932.0 | 940.3 | +0.9% | 904.9-959.8 | 915.4-1,022.0 |
| Ingest p50 / p95 ms | 708.3 / 868.1 | 711.5 / 819.4 | +0.5% / -5.6% | 680.7-735.5 / 773.9-892.6 | 649.7-747.3 / 690.3-871.2 |
| Projection p50 / p95 ms | 4,701.9 / 4,937.3 | 4,585.9 / 4,990.9 | -2.5% / +1.1% | 4,616.2-4,815.7 / 4,903.3-5,089.9 | 4,244.6-4,766.4 / 4,665.8-5,105.4 |
| Ingest WAL bytes per round | 20,777,437.5 | 20,777,434.5 | -0.00% | 20,777,403-20,777,451 | 20,777,413-20,777,439 |
| Projection WAL bytes per round | 34,190,624.5 | 34,186,625.0 | -0.01% | 34,174,008-34,196,596 | 34,173,428-34,200,076 |
| Go bytes per statistic, ingest / projection | 39,384.1 / 5,746.9 | 39,384.9 / 5,743.7 | +0.00% / -0.06% | 39,382.9-39,385.0 / 5,737.2-5,748.7 | 39,382.0-39,386.2 / 5,740.5-5,754.4 |
| Go allocations per statistic, ingest / projection | 233.2 / 60.1 | 233.2 / 60.1 | 0 | | |
| Peak heap in use after a round | 13.13 MB | 13.24 MB | +0.9% | 10.28-14.34 MB | 12.72-14.30 MB |
| Projection lag p50 / max ms | 1,487.1 / 2,310.0 | 1,531.2 / 2,323.9 | +3.0% / +0.6% | 1,359.8-2,024.5 / 2,048.9-2,513.0 | 1,357.8-2,217.8 / 1,994.6-2,668.4 |

The flags of batches 1 and 2 did not repeat in the next batch, are of the
size the A/A control shows between two runs of the same binary, and fall on
metrics the move cannot change: ingestion code did not move, and with
checkpoints held off the WAL of each round agrees to a few bytes. No
regression is attributable to the pilot. Limits: one machine, one PostgreSQL
configuration with `fsync=off`, and lag bounded by the routine's 1-second
tick, so lag compares the two revisions and does not predict production lag.

### D8: all three hot-path singletons have an owner

- `e99019b` gives the two checker batchers an owner, `checker.Runtime`,
  started from `app/bootstrap` in their own routine group and stopped after
  the checker workers and before the consumers.
- `e24dc03` gives the check usage meter and its snapshot an owner,
  `checkpace.Pacer`, the design's option 2: methods of the owner that take
  the snapshot (`pace.ChargeRequest(snapshot, ...)`). `app` constructs the one
  Pacer per process and passes it to both queues, to `bootstrap.Setup` (which
  configures it, runs it among the consumers and hands it to the checker
  runtime and the new `scraper.Dispatcher`) and to `server.OpenRoutes`. A nil
  snapshot still means "before the first load": every owner is due and
  routes use the fallback interval; no placeholder snapshot exists. A nil
  Pacer behaves the same and meters nothing; only `cmd/seed-dev-data` passes
  one, as it never ran pacing before. `checkpace` has no package-level
  variable left, `jobs/checker` two fewer, and the hot-path import pins are
  unchanged.

Gates of `e24dc03`:

| Gate | Evidence |
| --- | --- |
| One owner, constructed in `app`, no fallback | `TestOnlyAppConstructsAPacer` (only `internal/app/app.go` calls `checkpace.NewPacer`); `NewRuntime`, `NewDispatcher`, `Setup` and `OpenRoutes` reject a nil Pacer |
| Composition | `TestInstallPacedQueuesGivesBothQueuesTheProcessPacer` (both installed queues schedule with the Pacer's snapshot; fails when either gets another), `TestCheckerRuntimeChargesOnlyItsOwnPacer`, `TestDispatcherPacesWithItsOwnPacer`, `TestPacersShareNoState`, `TestNewPacerStartsNothingAndRunFlushesOnceMoreAtTheEnd` |
| Batching and flush | `TestRequestMeterChargesEachWorkspaceOncePerRequest` now reads the charges from the batch the Pacer flushes; the existing meter tests run against per-test Pacers |
| Stop order | `TestPacerFinalFlushFollowsCheckerWorkersAndBatchers`: with the real Pacer and checker runtime, the final flush begins after the workers and the batchers stopped and writes the last request charged |
| Nil snapshot | `TestNilSnapshotUsesTheFallbackAndKeepsMetering`, `TestSelectDueWorkspacesChecksEveryoneBeforeFirstSnapshot` |
| Hash reuse, command budget, payload | The three queue tests pass against a real Redis |
| Accounting | +0 cryptographic operations, JSON encodes, Redis commands and database queries per check; [hot-path-operation-baseline.md](../performance/hot-path-operation-baseline.md) |
| Measurements | Interleaved benchmarks in the same file. `ChargeRequest` alone is 0.6 ns (3.7%) slower per call in all 16 rounds; the checker's per-request path, `BenchmarkRequestMeterAdmit`, is not slower at any owner count; no benchmark allocates more |

## Scale regressions

The twelve opt-in scale regressions ran at the scales their notes document:
1,000,000 routes, 70,000 routes for bulk deletion, and 200,000 alert rules.
They ran on the original base `3f66f5d`, the completed D5 tree `f5baa1f` and
the pilot `59aab12`. The three read-model tests also ran on `0422769`, the
commit before the pilot. Revisions alternated within each test, on one
disposable PostgreSQL 17.9 container on the machine above
(`shared_buffers=512MB`, `work_mem=16MB`, `fsync=off`). For all but the three
read-model tests the server also had `checkpoint_timeout=1d` and
`max_wal_size=400GB`, with a `CHECKPOINT` before each run, so WAL figures
leave out full-page images written after a checkpoint. No assertion and no
scale was changed.

| Test | Outcome on every revision | Comparable measurements |
| --- | --- | --- |
| `TestReadModelProjectionScopesPopulatedEvidencePostgres` | fail | Projecting 5,000 routes x 8 owners exceeds the test's own 15-second budget on this machine. 87-90 s per run on all four revisions. |
| `TestReadModelPairCostPopulatedPostgres` | fail | Trial 1 expects 5,000 conflicting rows that the conflict filter rejects. The no-op filter, in the same commit as the test (`5f6e5a7`), now excludes them before the insert. Trial 0 with checkpoints held off: WAL 11.92-11.94 MB, 141,105-141,108 records, 0 full-page images, SQL 396-409 ms on all four. The first run had default checkpoints and logged 12 MB on `3f66f5d` and `f5baa1f` but 107-116 MB on `0422769` and `59aab12`; the controlled rerun shows that difference was full-page images. |
| `TestProxyFilterIndexUpdateCostPopulatedPostgres` | fail | Expects every pre-existing row updated; the same no-op filter skips unchanged rows. The three logged trials: WAL records identical across revisions (55,070-55,071, 54,575-54,578, 54,616-54,620), SQL 150-169 ms, one 97 ms trial on `f5baa1f`. |
| `TestStatisticsTransactionCostPopulatedPostgres` | pass | Three runs per revision. The statement counts of every variant are identical and WAL agrees within 0.6%. The 4-worker variants' medians and ranges overlap; the widest, at-tail with chunks of 5,000, is 13.9 s (12.7-17.6) on `3f66f5d`, 15.9 s (14.7-16.2) on `f5baa1f` and 14.4 s (13.2-16.2) on `59aab12`. |
| `TestSmallSourceHealthAggregatesCandidatesPostgres` | pass | 39-40 s. The aggregate's plan is the same except for one access path: an index-only scan of `user_proxies_pkey` on `3f66f5d`, an index scan of `idx_user_proxies_proxy_id` on `f5baa1f` and `59aab12`. It executes in 0.065 ms and 0.043-0.044 ms. One run each, so a statistics-sampling flip is not ruled out. |
| `TestCheckerRefreshAndRecentChecksScalePostgres` | pass | Reconciliation 1.7-2.1 ms; edits 4.8-64 ms; recent checks 2.3-3.0 ms. |
| `TestCheckerEvidencePrunePageBoundsPopulatedInventoryPostgres` | fail | The obsolete-prefix page deletes 0 pointers where the test expects some. The all-current page scans 50,000 rows in 507-510 ms (plan 519-528 ms) on all three revisions. |
| `TestProxyFilterOptionsScalePostgres` | pass | Its measured queries run on the test's own table and touch no production code, and its file is identical on `0422769` and `59aab12`. Median of ten reads per run: `3f66f5d` 98-101 ms (4 runs), `f5baa1f` 102-107 (4), `0422769` 98-158 (7, of which 3 at 125-158), `59aab12` 102-136 (8, of which 5 at 120-136). Both of the last two alternate between about 100 ms and 120-160 ms, and the slow runs cluster in time, so this measures the machine's own variation. |
| `TestRotatorEvidenceScopesSharedInventoryPostgres` | fail | Its scoped count visits all 999,999 routes and 4.0 million latest rows on every revision. |
| `TestSnapshotAliveCountsScopeSharedInventoryPostgres` | pass | The 8-owner scoped count, SQL median over 4 runs: 3,815 ms on `3f66f5d`, 3,949 on `f5baa1f`, 3,760 on `0422769` (3 runs) and 3,930 on `59aab12`; every pilot run lies inside the other revisions' range. 1 owner: 824-895 ms on all. The baseline query of the old version times out at 15 s on all (logged, not asserted). |
| `TestBulkProxyDeletionPostgres` | pass | Statement counts identical; 3.5-3.8 s. |
| `TestAlertsScaleCadencePostgres` | pass | 200,000 rules per pass, pass durations 14.7-22.1 s in the same pattern on all three. |

No test changes outcome between the original base, the D5 tree and the pilot,
and no comparable measurement shows a difference beyond what the
measurement-only filter-options test shows between runs of one revision.
Five of the twelve fail already on `3f66f5d` in this environment; see the open
items. Limits: one machine, `fsync=off`, and one to four runs per revision for
these long tests, apart from the harness above.

## Items still open

These spec items are deferred; none blocks the commits above.

- **No further core splits.** The stop rule fired after the `readmodel`
  pilot. The rest of `internal/database` (11,202 lines) stays one package.
- **Owners for the other stateful packages.** ADR 0015's end state is one
  owner per stateful package, constructed in `app`. The three hot-path
  singletons have one now. `rules/state.txt` still counts package-level state
  elsewhere, for example `app/server` 41, `config` 18, `geolite` 18,
  `support` 17, `jobs/checker` 14 (its HTTP-client and regex caches),
  `jobs/runtime` 12, `jobs/scraper` 11, `rotatingproxy` 10 and the two queue
  singletons `PublicProxyQueue` and `PublicScrapeSiteQueue`, which `app`
  installs with the process's Pacer. These counts can only fall.
- **Transaction allowlist** (`rules/transactions.txt`): the four schema
  migrations that open a transaction on the handle they receive, and
  `devseed.SeedDevelopmentData`. Each moved unchanged; each needs the
  follow-up logic change the design describes.
- **Connection allowlist**: `support/redis_client.go` still creates the
  Redis clients outside `internal/platform`.
- **Layer violation** `database -> jobs/checker/judges`: checker settings
  read the judge cache; it stays while checker-settings SQL stays in the
  core.
- **`alerts.CloseForRotator`**: the design names this entry point; the move
  kept `CloseAlertForConfiguration` and the rotator's loop in the core.
  Folding the loop into one alerts function is a logic change.
- **`readmodel` names.** The pure move kept every name, so several read
  `readmodel.RefreshReadModelIndexes`-style. Shorter names would be a
  rename-only change of their own.
- **Five opt-in scale regressions fail on the original base.** At their
  documented scales on this machine, `TestReadModelPairCostPopulatedPostgres`
  and `TestProxyFilterIndexUpdateCostPopulatedPostgres` still expect the
  conflict filter to see rows that the later no-op filter excludes;
  `TestReadModelProjectionScopesPopulatedEvidencePostgres` misses its
  15-second budget; `TestCheckerEvidencePrunePageBoundsPopulatedInventoryPostgres`
  deletes no obsolete pointers; and
  `TestRotatorEvidenceScopesSharedInventoryPostgres` visits the whole shared
  inventory. Each fails the same way on every revision measured. Updating
  them is test work outside this change.
- **Browser scraper tests** need a local Chromium and did not run.
