# Bound obsolete latest-evidence pruning

The populated release rehearsal timed out in statistics retention while its
latest-pointer delete expanded the global current-evidence query. Its limit
applied after finding obsolete rows. A current-only table could therefore scan
all pointers without deleting anything. Each of the four history workers
repeated that work.

Maintenance now scans primary-key candidate pages before checking eligibility.
The first page captures an upper key, and subsequent pages seek between the
previous key and that fixed boundary. A successful page advances even when it
deletes nothing. The cursor survives a maintenance interval's batch or duration
budget; reaching the captured end clears it. The next cycle revisits settings
changes behind the cursor. Newly appended keys cannot keep a finite cycle open.
Cancellation fails the statement without advancing the saved cursor.

Eligibility checks use the candidate's workspace, owner and committed checker
key. Ordered primary-key successor probes verify equality after limiting the
lookup to one row. This avoids scanning other owners through the proxy-only
index. Candidate pages contain only keys. Obsolete pointers receive a separate
verified primary-key lookup for their physical tuple IDs, and a TID-array delete
removes those tuples without a second global table scan. No new index is needed.

The coordinator runs one latest-evidence page per history-worker round, rather
than one global prune per worker. History deletion and response-body pruning
remain concurrent. The existing 50,000 batch size, four workers, 240 history
batches per run, four-minute run budget, five-minute interval and 45-second
operation deadlines are unchanged. With four workers, 240 history batches allow
up to 60 candidate pages per run; unfinished finite scans resume next interval.

Current sparse/default keys remain pinned. Pending workspaces retain all their
modern pointers. Legacy namespace-zero pointers remain pinned while any
generation-zero owner exists, including paused or archived ownership. Modern
paused ownership also retains its matching evidence. Empty or stale modern keys,
removed owners and removed workspaces become eligible for pruning. Transport
does not add a new retention predicate. Detailed-history deletion still protects
every remaining latest pointer, and event replay identities are unchanged.

`PruneObsoleteCheckerEvidence` retains its existing delete-limit interface for
direct callers, with local eligibility predicates. The maintenance page API
uses PostgreSQL's data-modifying CTEs and physical tuple IDs. Its SQLite fallback
retains the portable delete-limit behavior.

## Verification

The PostgreSQL semantic regressions cover key overrides, disabled keys, dirty
workspaces, legacy ownership, lifecycle states, removed owners and workspaces,
cancellation, finite-cycle completion, newly appended keys and revisiting
invalidations behind the cursor. A concurrent regression runs four history
retention workers alongside pruning and verifies that every current pointer
still has its historical row. Existing replay/idempotence and dirty-projection
retention regressions also pass.
The final focused race run passed, including portable SQLite behavior, old-row
history pins and response-body retention. Exact commands and results are in
`evidence-pruning/targeted-race.json` and `targeted-race.log`; the database package
completed in 12.008 seconds. The runtime package also compiled successfully.

The populated fixture restores and validates all four original foreign keys
before measurement. It uses the original index set in a disposable PostgreSQL
17 Alpine container limited to two CPU slots and 3 GiB memory, with 256 MiB shared
buffers and 16 MiB work memory. It tests a 50,000-candidate current-only page and
an obsolete prefix, including analyzed execution plans. Plan accounting counts
physical scan rows once; `ModifyTable` returning rows are output, rather than
another physical scan.

At 80,000 latest pointers and 80,000 ownership rows, the current-only page took
0.631 seconds and the page deleting 156 obsolete pointers took 0.562 seconds.
Both visited exactly 50,000 ownership rows. For this small fixture the candidate
page contains most of the table, so PostgreSQL prefers a sequential scan of the
80,000 pointers. The test allows that only when the entire inventory is smaller
than two pages. Larger fixtures must visit only the candidate page, its boundary
key, and one pointer lookup plus one TID read per obsolete entry. Ownership
visits may never exceed the page size.

The representative run contained 1,000,000 routes, 8,000,000 ownership rows and
8,000,000 latest pointers, with the same original indexes and container limits:

| Operation | Elapsed | Analyzed execution | Latest scan rows | Ownership scan rows |
| --- | ---: | ---: | ---: | ---: |
| Original all-current query | Canceled at 15.015 s | Planned only | Not measured | Not measured |
| Current-only 50,000-candidate page | 0.647 s | 0.515 s | 50,001 | 50,000 |
| Page deleting 15,625 obsolete pointers | 0.738 s | 1.070 s | 81,251 | 50,000 |

The current-only plan visits its 50,000 candidate keys and one boundary key. The
obsolete-prefix plan adds exactly one primary-key pointer lookup and one TID
read per deletion. It performs no full latest-table or ownership scan. Both
large plan bounds are asserted by the test. The separate analyzed executions
incur instrumentation overhead and different cache state; these single-run
samples are not latency distributions. The analyzed delete is rolled back
before the timed production delete. The baseline deadline establishes a lower
bound and does not reproduce the full application incident. The complete large
fixture and test completed in 109.817 seconds.

Run the checks against a disposable PostgreSQL database:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32772 user=magpie_maintenance_test password=magpie_maintenance_test dbname=magpie_maintenance_test sslmode=disable' \
  go test ./internal/database -run 'TestCheckerEvidencePrune|TestStatisticsReplayIsIdempotent|TestPendingCheckerProjectionSuppressesHealthWithoutPruningEvidencePostgres' -count=1 -v

MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32772 user=magpie_maintenance_test password=magpie_maintenance_test dbname=magpie_maintenance_test sslmode=disable' \
MAGPIE_TEST_EVIDENCE_PRUNE_ROUTES=1000000 \
  go test ./internal/database -run '^TestCheckerEvidencePrunePageBoundsPopulatedInventoryPostgres$' -count=1 -v -timeout=20m
```

`MAGPIE_TEST_EVIDENCE_PRUNE_EVIDENCE_DIR` saves SQL, plans and timings. Optional
`MAGPIE_TEST_EVIDENCE_PRUNE_BASELINE_SQL` runs the retained original query with a
fifteen-second comparison deadline. Accepted evidence is under
`.codex-tmp/release-remediation-20261020/evidence-pruning/default-final/` and
`million/` in the workspace. Earlier diagnostic directories are excluded.

These changes add zero cryptographic operations, JSON encodes, Redis commands or
database statements to each proxy check. Queue payload, transport and replay
behavior are unchanged. This query regression does not establish application
soak capacity. The coordinated validation driver owns the final combined Go
checks and release rehearsal.
