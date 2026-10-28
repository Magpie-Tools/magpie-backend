# Proxy page and filter-option reads at shared-inventory scale

The local release rehearsal contains one million routes shared by eight
workspaces. Its first proxy page took 5.5 seconds during startup. PostgreSQL
statistics already estimated about one million projection rows per workspace;
the slow plan was not caused by missing statistics.

The default page ordered the masked `alive` output alias, which required sorting
the entire selected workspace. The full existing index also had mixed sort
directions (`alive ASC, latest_check DESC`), while the default page needs both
columns descending. The matching descending index was partial to active
management, but the unfiltered page includes paused and archived routes too.

## Changes

The full `idx_workspace_proxy_filter_page` index matches
`workspace_id, alive DESC, latest_check DESC, proxy_id ASC`. Default ordering now
uses the qualified projection columns and a stable proxy-ID tie breaker.
Response-time, anonymity, alive, health and latest-check masks still hide stale
health while checker settings are pending. Lifecycle filtering and the existing
health-filter guard are unchanged.

Exact unfiltered counts now read `user_proxies` directly. Its foreign keys already
establish the owned route relationship, so counting no longer joins and scans
the global route table. The count still includes active, paused and archived
management. GraphQL uses the count returned with its page instead of issuing the
same count twice on populated workspaces. A zero result still obtains the
independent ownership count, preserving the existing total when a projection
read fails. Empty workspaces can therefore perform a second cheap count.

Filter options use three workspace-scoped expression indexes, one each for
country, estimated type and anonymity. Each has the same
`COALESCE(NULLIF(value, ''), 'N/A')` expression as the existing distinct query.
A recursive PostgreSQL query seeks the first value, then the next value greater
than the previous one. It reads the distinct values without scanning every
route's copy of them. The three option sets share one statement and snapshot.
Tags retain their existing fresh scoped read.

Original spelling, casing, SQL-NULL/empty normalization, whitespace exclusion,
database collation order and all-lifecycle option scope are preserved. Other
database dialects retain ordinary scoped `DISTINCT` queries in one `UNION ALL`
statement. No option cache or invalidation policy was added.

Index creation is concurrent. On retry, startup removes only invalid indexes on
the projection table whose names match these four new indexes, then recreates
them. A different valid index with the same name is preserved.

## Measurements and regressions

`TestProxyFilterOptionsScalePostgres` compares three candidates using eight
million option rows: one million per workspace. It includes mixed case, Unicode,
empty and whitespace values and checks exact result values and database order.
The fixture holds only the columns needed by these covering indexes; it does not
represent the size or cost of the complete projection heap.

An uncontended local PostgreSQL 17 comparison gave these ten-read timings:

| Candidate | Per-read elapsed time | Added option indexes |
| --- | --- | --- |
| Three scoped scans of one covering index | 173–197 ms | One, 58 MB |
| One covering distinct-triple scan, then distinct option extraction | 105–115 ms | One, 58 MB |
| Indexed successor seeks | First 2.3 ms; warm 0.48–0.86 ms | Three, 170 MB total |

The successor query was chosen for the representative workload's ten filter
reads per second. Its work follows option cardinality rather than route count.
These local comparison times are not an API latency guarantee; the complete
representative load and soak remain separate release gates.

`TestProxyPageAndCountUseWorkspaceIndexesPostgres` exercises the actual page,
count and filter helpers with 10,000 routes, eight owners, mixed lifecycle states,
alive/dead rows and repeated timestamp ties. Its recorded plans show no full page
sort, 40 projection rows for the first page, only the selected workspace's 10,000
ownership rows for the exact count, and indexed option seeks. Local SQL execution
times were about 0.08 ms, 0.65 ms and 0.17 ms respectively. The exact count remains
linear in selected ownership; no approximate count was introduced.

Additional regressions check deterministic pagination, pending masks, lifecycle
scope, error propagation, GraphQL count reuse, interrupted concurrent-index
recovery, migration idempotence and preservation of valid customer indexes.

Run the focused checks against a disposable PostgreSQL database:

```bash
MAGPIE_TEST_POSTGRES_DSN='host=... dbname=... user=... password=...' \
MAGPIE_TEST_FILTER_ROUTES=1000000 \
MAGPIE_TEST_PROXY_READ_EVIDENCE_DIR=/absolute/path/to/evidence \
  go test ./internal/database ./internal/graphql \
  -run '^(TestProxyFilterOptions|TestProxyPage|TestProxyListPerformanceSchema|TestBuildProxyPage)' \
  -count=1 -v
```

Plans use `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` only inside temporary schemas
in that disposable database. The real release fixture was inspected read-only.

## Write cost and checker operation budget

These four indexes add projection maintenance work. The accompanying duplicate
index cleanup removes thirteen exact copies of existing projection indexes.
The final projection-update comparison includes both changes. On one million
routes and eight million existing projection rows, the 5,000-route update median
SQL time changed from 6.48 seconds with the original 32 indexes to 6.02 seconds
with the final 23. Mean WAL records fell by 25.7 percent despite the four new
read indexes. See [the projection index comparison](projection-index-updates-2026-10-21.md)
for both batch sizes, repeated runs and limits of the measurement.

This changes API read SQL and projection indexes only. It adds zero database
queries, cryptographic operations, route hashes, JSON encodes or Redis commands
to a proxy check. Queue credentials, payload compatibility and normal requeue
behavior are unchanged.
