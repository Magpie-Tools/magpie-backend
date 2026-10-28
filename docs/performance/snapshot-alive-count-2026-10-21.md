# Count current alive routes before joining ownership

The restored million-route workload showed the snapshot alive-count query still
running after 123 seconds. PostgreSQL correlated the current overall-status
aggregate to every active ownership row. Across eight shared workspaces, this
repeated the same evidence lookup millions of times and competed with projection
refreshes during startup.

`aliveProxyCountByWorkspace` now selects distinct route pairs with current alive
evidence for the requested workspaces, then joins those pairs to active
ownership. A separate branch reads `proxy_overall_statuses` for generation-zero
workspaces. Both branches run in the same statement. A route with several alive
protocols contributes one count, and routes without alive evidence require no
correlated aggregate.

The query preserves the public health rules: pending workspace settings mask all
current health, sparse plan keys override workspace defaults, empty keys disable
modern evidence, and stale or other-workspace keys do not count. Any current
alive protocol counts, including QUIC. Paused and archived ownership does not
count. Generation-zero workspaces continue to use legacy overall status even
when their latest protocol rows disagree with it. Workspaces without matching
routes return zero.

`TestSnapshotAliveCountsPreserveCurrentEvidence` exercises those rules on SQLite
and PostgreSQL and compares its results with the unchanged public overall-status
query. `TestSnapshotAliveCountsScopeSharedInventoryPostgres` loads eight shared
owners, populated current and obsolete pointers, and unrelated owners for the
single-workspace probe. It asserts that one grouped statement replaces the
per-route lateral aggregate, verifies exact counts, and inspects actual rows
visited in PostgreSQL plans. Its default fixture has 10,000 routes; set
`MAGPIE_TEST_SNAPSHOT_ROUTES=1000000` for the representative probe. The synthetic
bulk load restores and validates its original foreign keys before measurement.

The uncontended 10,000-route fixture had 80,000 active ownership rows and 40,000
latest pointers. With PostgreSQL 17 limited to four CPU slots, 5 GiB memory,
512 MiB shared buffers and 16 MiB work memory, the former eight-owner query took
10.730 seconds. The replacement took 29.550 milliseconds for all eight owners
and 5.374 milliseconds for one owner, returning 1,250 alive routes per owner.
The single-owner analyzed plan visited 5,000 latest pointers and 1,250 ownership
rows, without looking up the other owners' pointers. These are single-run
latency samples; plan instrumentation and repeated measurements have separate
costs.

The uncontended million-route fixture contained eight million active ownership
rows and four million latest pointers. It used the same container limits and
returned exactly 125,000 alive routes for each requested owner:

| Query | Elapsed | Analyzed plan execution |
| --- | ---: | ---: |
| Former correlated count, eight owners | Canceled at 15.009 s | Planned only |
| Grouped count, one owner | 0.922 s | 0.761 s |
| Grouped count, eight owners | 4.353 s | 6.391 s |

The single-owner plan visited 500,000 latest pointers and 125,000 ownership rows.
The eight-owner plan visited approximately four million pointers and five
million ownership rows, rather than executing an overall-status aggregate once
per owned route. PostgreSQL instrumentation and changed cache state account for
the separately reported plan runtimes; neither column is a repeated latency
distribution. The fifteen-second baseline limit establishes a lower bound,
without reproducing the full 123-second startup query. Evidence is retained in
`.codex-tmp/release-remediation-20261020/snapshot-count/default/` and `million/`
in the workspace. The earlier `initial-contended/` run overlapped another
synthetic fixture and is excluded from these measurements.

Run against disposable PostgreSQL:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_read_model_test password=magpie_read_model_test dbname=magpie_read_model_test sslmode=disable' \
  go test ./internal/database -run '^TestSnapshotAliveCounts' -count=1 -v

MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32770 user=magpie_read_model_test password=magpie_read_model_test dbname=magpie_read_model_test sslmode=disable' \
MAGPIE_TEST_SNAPSHOT_ROUTES=1000000 MAGPIE_TEST_SNAPSHOT_BASELINE=1 \
  go test ./internal/database -run '^TestSnapshotAliveCountsScopeSharedInventoryPostgres$' -count=1 -v -timeout=20m
```

`MAGPIE_TEST_SNAPSHOT_EVIDENCE_DIR` saves plans and timings. The optional baseline
executes the former correlated query with a fifteen-second deadline. These are
query measurements, not a completed application soak or a production capacity
guarantee.

The change is confined to snapshot and dashboard count reads. It adds zero
cryptographic operations, JSON encodes, Redis commands, or database statements
per proxy check. It uses no proxy credentials, changes no queue behavior, and
leaves the public current-evidence helpers unchanged. Full combined Go validation
must follow the remaining release-rehearsal query and index changes.
