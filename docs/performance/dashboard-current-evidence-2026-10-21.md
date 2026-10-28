# Dashboard evidence reads and cancellation

The migrated million-route rehearsal still overloaded PostgreSQL before its HTTP
preflight. Dashboard prewarm refreshed eight workspaces, and a judge-anonymity
query took about 25 seconds while other database work was active. Shutdown also
left a fastest-proxy query running because its database calls had lost the
producer's context.

## Query changes

Workspace reads now select the evidence namespace with a CASE expression before
validating current keys. Generation zero selects namespace zero; compiled
workspaces select their own ID. An explicit namespace guard gives PostgreSQL a
filter it can apply before ownership and sparse-override joins. The default
helper includes every owned lifecycle state. Dashboard judge and fastest reads
use its active-only form and no longer join ownership a second time.

This retains dirty-workspace masking, default keys, sparse overrides, disabled
empty keys, legacy evidence, unknown protocol IDs with explicit overrides and
both TCP and QUIC evidence. Judge assignment and anonymity-level eligibility
remain in the judge aggregate.

The fastest query first selects the same winning alive result per proxy. Its
ordering remains response time ascending, checked time descending and statistic
ID descending. It then materializes the requested top candidates by response
time, checked time and proxy ID. Proxy and reputation metadata load after this
limit, because those fields do not affect eligibility or order. PostgreSQL's
existing NULL ordering is preserved; NULL response times do not become zero
until response formatting. The dashboard and GraphQL limit remains 100.

The global current-evidence view remains available for callers that require a
global read. No index was added for this change.

## Actual fixture comparison

The backend was stopped before measurement. Every query ran in a read-only
transaction with a 30-second statement timeout. The fixture had one million
routes shared by eight workspaces and more than seven million latest rows.
Normal scheduled autovacuum was allowed to continue. No manual vacuum, analyze,
planner setting or resource-limit change was made.

| Query | Previous SQL execution | New SQL execution |
| --- | ---: | ---: |
| Judge anonymity aggregate | 7.37 s | 1.95 s |
| Fastest 100, including metadata | 24.41 s | 2.21 s |

These timings came from `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`. Separate direct
SELECT runs returned exactly the same judge buckets and ordered fastest 100.
Those runs took 8.88 versus 4.07 seconds for judges and 14.08 versus 2.12 seconds
for fastest proxies. This variation reflects cache warmth and ongoing vacuum;
the numbers are local comparisons, not a production latency guarantee.

The old fastest plan performed about one million reputation lookups before
limiting its response to 100 routes. That enrichment was the largest avoidable
cost. A default-key/sparse-override UNION alternative took 5.85 seconds for judges
and 12.75 seconds for fastest proxies, so it was rejected. The simpler scoped
helper plus bounded enrichment needed no additional write index.

The new scoped evidence query can still scan the latest heap. Exact aggregate
work and per-proxy winner selection remain proportional to selected inventory.
The combined HTTP load and soak are separate release gates and must verify that
background refresh leaves enough capacity for other work.

## Context and cache publication

`RefreshDashboardCaches(ctx)` now passes a contextual database handle through
health-version checks, all dashboard-info branches, daily and fallback check
counts, snapshots, current scraped counts, recent checks and fastest proxies.
Delayed daily backfill captures the same database handle and stops its delay or
transaction when that context ends. Public wrapper signatures are unchanged.

Refresh returns query errors. A failed dashboard-info branch cannot publish a
partial snapshot, and a canceled recent or fastest read cannot replace the last
successful cached list. Successful reads still recheck the committed checker
generation before publishing health.

## Regression evidence

`TestWorkspaceLatestStatisticsMatchesLegacyEligibility` compares complete rows
against the unchanged global view on SQLite and PostgreSQL. It includes current
and stale results, default and sparse keys, disabled keys, shared ownership,
orphan evidence, active/paused/archived management, dirty and legacy workspaces,
mixed transports and an unknown protocol with an explicit override.

`TestDashboardScopedReadsPreserveFastestAndJudgeResultsPostgres` compares actual
helper results with the previous queries. It checks limits 1, 3 and 100, multiple
results per proxy, response/check-time ties, NULL response times, reputation
normalization and assigned-judge buckets.

`TestDashboardFastestMetadataIsBoundedPostgres` captures the SQL emitted by the
production helper and explains that SQL with 10,000 unrelated routes and
reputations present. For a requested limit of three, the plan performs three
proxy lookups and three reputation lookups, with no metadata-table scan.

`TestDashboardRefreshCancelsBlockedQueriesPostgres` holds a real PostgreSQL table
lock. It observes the refresh waiting on that lock, cancels the supplied context,
then verifies that the query ends and all previous cached snapshots survive.
It covers the complete job plus info, recent and fastest loaders separately.
Observed cancellation and backend-query disappearance took about 2 to 13 ms in
the focused run. A separate failed-component test verifies that ordinary query
errors also preserve the previous dashboard snapshot.

Run the focused checks against a disposable database:

```bash
MAGPIE_TEST_POSTGRES_DSN='host=... dbname=... user=... password=...' \
MAGPIE_TEST_PROXY_READ_EVIDENCE_DIR=/absolute/path/to/evidence \
  go test -race ./internal/database \
  -run '^(TestWorkspaceLatestStatistics|TestDashboardScopedReads|TestDashboardRefreshCancels|TestDashboardInfoQueryFailure|TestDashboardFastestMetadata)' \
  -count=1 -v
```

This changes API/background read SQL and context propagation. It adds zero
database queries, cipher operations, route hashes, JSON encodes or Redis commands
to the checker loop, and adds no latest-evidence index maintenance.
