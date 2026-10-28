# Dashboard ownership joins on the retained million-route fixture

Driver c passed the complete Go, race, vet, build, image, documentation and
distribution checks, then failed backend readiness after 30 minutes. Dashboard
prewarm still ran before the API and background checker jobs. Cancellation found
it loading workspace 8's judge aggregate. This identifies the last incomplete
loader; the log does not establish that one query occupied all 30 minutes.

The stopped fixture reproduced an expensive ownership join in both the judge
aggregate and fastest-route query. Their current-key estimate admitted about
2,000 rows, while the actual judge aggregate admitted 990,501. PostgreSQL chose a
route-only ownership index probe for each result, filtering workspace and active
state afterwards. Shared ownership made those probes visit other owners too.
The exact ownership primary key already existed, but adding predicates did not
make the planner select it.

## Read change

Dashboard judge and fastest loaders now materialize the selected workspace's
active proxy IDs once and join current evidence against that set. This prevents
the repeated ownership probes without adding an index. The set comes from
authoritative `user_proxies`, not the derived list projection.

The common key predicate is shared with the existing helper. Generation zero
still selects namespace zero without validating keys. Compiled workspaces use
their own namespace and nonempty current default or sparse override keys; empty
overrides disable evidence, including default-key results. Dirty workspaces
remain masked. Unknown protocols with explicit keys, TCP and QUIC, assigned
judges and anonymity-level grouping retain their previous semantics. Fastest
selection keeps its per-route winner, PostgreSQL NULL ordering, ties and bounded
metadata enrichment. Recent checks already use a bounded candidate list and are
unchanged.

Point/detail/export reads retain the existing pushdown-friendly helper. They do
not materialize an entire workspace to read one route. SQLite retains its
previous compatible query. This change adds no index maintenance, persistent
writes or SQL statement. Each judge, fastest and recent query remains one SQL
statement. Added operations per physical proxy check are zero for cryptography,
route hashes, JSON, Redis and database access.

## Actual-fixture probes

The backend remained stopped. Probes used read-only transactions, 15-second SQL
and two-second lock deadlines, and rolled back. No baseline data, schema,
planner configuration, vacuum/analyze, environment, key or resource change was
made for these timing probes. The first old-judge probe ran while the user's
separate development application was active. The parent then stopped that
application with its existing authorization; the unchanged judge query still
hit the same 15-second bound. Subsequent comparisons used the quiesced host.
IDE and unrelated services remained present.

PostgreSQL retained its four-CPU/4-GiB limit, 128-MiB shared buffers and 4-MiB
work memory. Catalog estimates were about 8.03 million latest rows, 8.01 million
ownership rows, 7.36 million projection rows and 2.01 million reputation rows.
These are estimates, not an assertion that rows disappeared. Full heap sizes
were about 2.00 GB for latest, 2.16 GB for ownership, 1.83 GB for projection and
1.52 GB for reputation. The sparse-plan table was empty. All eight workspaces
were compiled and clean with one current HTTP key. Dead versions, table packing,
cache state and normal autovacuum differ from the earlier thin measurements.
Separate bounded exact counts confirmed 1,000,001 proxies, 8,000,008 ownership
and projection rows, 7,999,994 latest rows and 2,000,002 reputations. Each workspace
owns 999,999 active, one paused and one archived route. Workspace 2 has 1,000,001
current-key latest rows; the other seven each have 999,999. Active-owner
eligibility yields 990,501 alive results for the measured judge aggregate.
The baseline's combined memory/swap limit is 8 GiB; the separate regression
service has a 4-GiB combined limit. Both have a 4-GiB memory limit.

SQL was captured from the production loaders on a separate PostgreSQL fixture.
Only workspace arguments and the fastest limit changed for actual-fixture
probes. The old SQL uses the unchanged prior evidence helper inside the same
outer production query.

| Generated query | Previous plan | Materialized ownership |
| --- | ---: | ---: |
| Judge anonymity, workspace 8 | Exceeded 15-second deadline | 5.226 s |
| Fastest 100, workspace 8 | Exceeded 15-second deadline | 5.011 s |
| Recent eight checks, workspace 8 | Unchanged | 7.665 ms |

These are analyzed-plan execution times. The new judge and fastest each scan
ownership once. Their chosen plans still visit the full roughly eight-million
ownership and latest heaps; materialization removes the repeated random probes,
not all inventory-sized work. They spill temporary data with the fixed work
memory. A more complicated key-range/UNION alternative still scanned those
heaps and took 6.000 seconds, so it was rejected.

The old and new actual-fixture SELECTs returned exactly equal judge buckets and
the same ordered fastest 100, including metadata. To complete the old SELECTs
within the same 15-second budget, the equality-only transactions used
`SET LOCAL enable_nestloop=off`, then rolled back. This changes the execution
strategy without changing SQL eligibility or results. Those forced-plan runs
are excluded from the timing comparison; the unmodified old-plan timeouts remain
retained.

## Whole-pass limit

A temporary Go test overlay invoked the actual info, recent and fastest loaders
serially for all eight workspaces, as the production refresh does. Its connections
enforced `default_transaction_read_only=on`, a 15-second per-statement deadline
and a three-minute total context. All 24 loaders succeeded in 111.982 seconds:

| Loader | Per-workspace range |
| --- | ---: |
| Info, with its six concurrent branches | 7.193–11.888 s |
| Recent eight | 0.012–0.052 s |
| Fastest 100 | 3.947–5.154 s |

The existing delayed daily backfill ran its reads but encountered missing
markers. PostgreSQL denied eight attempted inserts because the diagnostic was
read-only; the warnings remain in the evidence. Its write cost is therefore
excluded. The overlay is a quiet read diagnostic, not a production startup or
concurrent capacity pass. It changes no shipped source or runtime deadline.

The observed pass exceeds the unchanged 30-second refresh interval. Long passes
still run back-to-back under the existing ticker. This fix does not establish
refresh cadence, checker throughput or the sustained gate. The separate startup
change bounds optional prewarm; clean recovery and all original load/SLO checks
remain required.

## Regression and evidence

The common eligibility regression compares complete rows against the global
reference on SQLite and PostgreSQL, now including the dashboard helper. Existing
dashboard regressions compare judge buckets and ordered fastest results for
limits 1, 3 and 100, and preserve cached snapshots after query failure or real
PostgreSQL lock cancellation. The new populated plan regression uses 10,000
routes shared by eight owners and asserts one ownership scan for the generated
judge and fastest queries, while checking complete judge counts and stable ties.
Focused normal and race commands passed in 12.382 and 18.634 seconds, with twelve
PASS entries and no skips each. Real blocked-query cancellation completed in
about 10.9–12.2 ms and retained the previous cached snapshots.

Focused normal and race checks, complete generated queries, analyzed plans,
timed-out old queries, per-workspace results, resource/catalog captures and
source hashes are retained in
`.codex-tmp/release-remediation-20261020/dashboard-startup-20261022/`.

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_dashboard_test password=magpie_dashboard_test dbname=magpie_dashboard_test sslmode=disable' \
  GOTOOLCHAIN=go1.27.3 go test -race ./internal/database \
  -run '^(TestWorkspaceLatestStatisticsMatchesLegacyEligibility|TestDashboardScopedReadsPreserveFastestAndJudgeResultsPostgres|TestDashboardFastestMetadataIsBoundedPostgres|TestDashboardRefreshCancelsBlockedQueriesPostgres|TestDashboardInfoQueryFailureKeepsPreviousSnapshotPostgres|TestDashboardWholeInventoryOwnershipPlanPostgres)$' \
  -count=1 -timeout 5m -v
```

The disposable regression service is separate from the retained baseline and
uses the same four-CPU/4-GiB, 128-MiB shared-buffer and 4-MiB work-memory limits.
After the focused runs it was renamed to the parent's expected
`magpie-read-model-pg-20261021`, with a compatibility test role/database added.
A separate disposable Redis was recreated on port 32771 so the parent's full
suite and later quiesce stage can use their existing fixture names. The original
dashboard test database remains available. This handoff changed no baseline
service or application source.
The parent owns the next full combined Go checks, local-image rehearsal,
coordinated recovery, smoke and sustained load. Item 4 remains deferred.
