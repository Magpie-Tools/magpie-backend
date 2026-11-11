# Verification log of the architecture v4 implementation

This is the durable record of what was checked for each commit of the staged
extraction described in
[ADR 0015](../adr/0015-extract-feature-packages-behind-architecture-tests.md):
the per-commit suites, the SQL-log comparisons, the onboarding checkpoints and
the gates that are still open. Every number here was measured; anything not
measured is listed as such.

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
final head, as recorded below.

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

Notes:

- `7edec44`: the first integration run timed out in
  `TestProxyIngestionBatchesPostgres` while a second, unrelated test run was
  using the same PostgreSQL server. Re-run alone on the same commit, it
  passed (the table shows the re-run). After that, foreground runs used a
  second disposable service pair so verification never shared a server.
- The integration runs skip the browser scraper tests, which need a local
  Chromium, and the opt-in scale regressions (`MAGPIE_TEST_*_ROUTES`,
  `MAGPIE_TEST_ALERT_SCALE`). Neither was run for any commit.

## SQL-log comparisons

The procedure is in [sql-log.md](sql-log.md). Digests group statements by
backend process and virtual transaction ID and count each distinct
per-transaction sequence.

| Reference | What it covers | Stability proof |
| --- | --- | --- |
| `3f66f5d` / `b50b951` | The initial test list: 7855 transactions, 541 distinct sequences, 12182 statements | Three runs of `3f66f5d` byte-identical; `b50b951` identical to them |
| `163e58f` | Same list. The test kit renamed the test schemas from `proxy_ingestion_batches_N` to `magpie_test_N`, so the `CREATE SCHEMA` and `DROP SCHEMA` lines differ from `b50b951`; every other line is identical | Bodies identical to `b50b951` after that rename |
| `c226b99` | The list plus the source tag test, added before the scraper store moved: 8087 transactions, 554 distinct sequences, 12474 statements | Two runs of `c226b99` byte-identical |

Every extraction commit's digest is byte-identical to the reference of its
test list. The commits from `e9d6dca` to `ebd31ec` were first compared with
`b50b951` and reported as different; the only difference is the schema name
introduced by `163e58f`, and each of them is byte-identical to `163e58f`.

## Onboarding checkpoints

Measured with `go run ./internal/tools/onboarding`; the procedure and the full
tables are in [README.md](README.md).

| Checkpoint | Commit | Must-read tokens | Change | Search-space tokens |
| --- | --- | ---: | ---: | ---: |
| Baseline | `3f66f5d` | 89,070 | | 902,720 |
| After step 0 | `b50b951` | 98,397 | +9,327 | 899,888 |
| After `alerts` | `7a51ea4` | 95,130 | -3,267 | 491,541 |
| After D5 | `f5baa1f` | 94,791 | -339 | 332,569 |

The stop rule ("stop extracting when a re-run shows no further drop") did not
fire at any checkpoint. The last drop is small (-0.4%) and comes from one
task, workspace invitations, whose anchors moved; tasks whose anchors stay put
grow with every row added to the `AGENTS.md` package map.

## Gates still open

### D7 `readmodel` pilot: not started, blocked at the public-API gate

The pilot was analyzed on `f5baa1f`, after the D5 checkpoint allowed it to
start. The read model (`read_model_indexes.go`, `read_model_batches.go`,
`read_model_projection_window.go`) can sit below the core: its only reference
into the rest of `internal/database` is the `DeleteChunkSize` constant, which
could move to `platform/pg`. The blocker is its tests.

- 31 test declarations in the read-model test files build their fixtures with
  core code (`InsertProxyStatistics`, `LoadCheckerWorkspace`) through the
  shared helper `setupSourceStatsShutdownTest`, so they would have to stay in
  `internal/database` (the `readmodel` package's tests cannot import the
  core).
- Those tests, and five other core tests, reach into the read model's
  unexported state. A pure move would need **21 new exported identifiers**:
  4 for production callers (`ensureReadModelBackfill`,
  `refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs`,
  `refreshWorkspaceScrapeSourceStatsForSites`,
  `refreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs`) and 17 for tests
  only, including the package's mutexes, dirty sets and round buffers
  (`readModelDirtyMu`, `readModelDirtyProxyIDs`, `readModelDirtyRound`,
  `readModelPendingBatch`, `sourceStatsDirty`, `sourceStatsDirtyMu`, ...).
- D7 requires "a reviewed public API (about 20 new exports triggers a
  boundary review)". 21 exports, most of them mutable state exposed for
  tests, trigger that review, which is a human decision.
- The scale gate ("repeated scale comparisons on matched hardware covering
  throughput, latency, WAL, memory and projection lag") has no harness yet.
  The existing read-model scale tests report elapsed time and plan shapes,
  not WAL, memory or projection lag.

Proposal: first a test-only change that rebuilds the read-model fixtures
without core code (direct inserts of latest statistics instead of
`InsertProxyStatistics`) and gives the remaining cross-package tests one
documented seam, so the move needs only the 4 production exports; and a
comparison test, committed before the move, that runs fixed rounds of
statistics ingestion plus projection drain and records throughput, ingest and
projection latency, `pg_wal_lsn_diff`, Go allocation and projection lag, to
be run alternately on the base and the pilot revision on this hardware.

### D8: checker batchers done; check usage meter and snapshot not started

`e99019b` gives the two checker batchers an owner (`checker.Runtime`),
starts them from `app/bootstrap` in their own routine group, and stops them
after the checker workers and before the meter's final flush. Its
composition, batching and stop-order tests pass, and its hot-path accounting
and interleaved benchmarks are in
[hot-path-operation-baseline.md](../performance/hot-path-operation-baseline.md).

The third singleton, the `checkpace` meter and snapshot, is not started. The
blocker is a design decision, not a failing gate: the snapshot's per-request
methods (`ChargeRequest`, `CheckDue`, `ScrapeDue`, `RouteCheckInterval`) read
process-wide state (the pause clock, the month cache, the meters and the
refresh channel). Giving that state an owner means one of:

1. an owner pointer on every snapshot, including a non-nil snapshot before
   the first load. That changes behavior where code relies on a nil snapshot
   (the requeue-all window and the checker's "no pace yet" path); or
2. owner methods that take the snapshot (`pacer.ChargeRequest(snapshot, ...)`),
   which changes signatures in the checker, both queues and the scraper.

Either way the owner must reach `PublicProxyQueue`, `PublicScrapeSiteQueue`
and the scraper, which are package singletons themselves. The change touches
three pinned hot-path packages and needs its own accounting and
measurements.

### Other open items

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
- **Scale tests**: none of the opt-in scale regressions ran for any commit.
  D10 requires them for core splits (D7), which did not happen.
