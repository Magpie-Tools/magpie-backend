# One inventory read for cold dashboard summaries

The snapshot-scope resume settled its strict catch-up sample at 1024/1024 after
12818.866 seconds, with zero due/outstanding work. Its next cold dashboard
request failed at 00:25:24 UTC on 27 October: HTTP 500 after 30001 ms and a client
`RemoteDisconnected`. Both observers had zero diagnostic errors. The application
stopped cleanly, exit 0, without OOM or restart. This remains failed preflight;
coordinated recovery, smoke and the 25-hour load gate did not start.

The request joined a delayed legacy daily-check backfill whose missing-marker
search exhausted the request context. A read-only invocation of the unchanged
loader also showed three concurrent queries scanning the same active inventory
projection for country counts, reputation counts and the highest-scoring proxy.
Each took approximately 7.92 seconds on stopped application data. Check counts
and judge aggregation also consumed substantial time. These observations do
not establish one cause of the live timeout.

The three inventory summaries now share one materialized active-workspace input
in one SQL statement. Each summary reads its selected columns from that input.
Country normalization and descending-count/ascending-country order, reputation
labels including unknown labels, null scores, top-score ties by ascending route
ID, metadata, tenant scope and active lifecycle remain unchanged. All summaries
see one statement snapshot. Query failure returns no partial summary.

The delayed backfill remains joined to its owner, with cancellation and retained
history semantics intact. The 30-second dashboard context, two-second backfill
delay, projection limits, indexes, PostgreSQL settings and resource profile are
unchanged. No prewarming or retry masks a cold request. The default physical
checker adds zero cryptographic operations, route hashes, JSON encodes, Redis
commands, credential loads or database queries. Cold inventory summaries use
one database round trip instead of three; no credentials are selected.

## Bounded measurements and their limits

One repeatable-read, read-only transaction executed the three original queries
and the shared query once each on the stopped million-route/eight-workspace
fixture. An execution-time assertion compared every returned country count,
reputation count and top-proxy field. They were equal. PostgreSQL retained its
4-CPU/4-GiB profile.

| Measurement | Three original queries | Shared input |
| --- | ---: | ---: |
| Base projection scans | 3 | 1 |
| Root shared read blocks | 682330 | 227229 |
| Root shared hit blocks | 30508 | 10341 |
| Root temporary read blocks | 0 | 14892 |
| Root temporary written blocks | 0 | 7446 |
| Comparison subplan time | 6.252 s | 2.622 s |

The original ran first and its three queries ran sequentially in this comparison,
while the application previously ran them concurrently. Cache state changed.
Shared reads are buffer operations, not physical disk-byte measurements. The
original read-only scan generated 71 WAL records for page hints; neither query
changed application rows. Materializing all requested active rows increases
temporary-file I/O. Fewer base-table reads are the measured reason for the
change; these timings establish no live speedup or capacity.

Separate read-only invocations of the complete cold loader took 15.087 seconds
before and 18.056 seconds after. Cache and concurrent test activity differed;
the candidate did not improve the observed whole-loader duration. Both probes
deliberately prevented optional backfill writes, so neither can qualify cold
readiness, backfill completion or live acceptance. Raw query timings and all
failures remain private in the remediation evidence.

Eighteen focused groups passed normally in 28.566 seconds and under race in
36.042 seconds, without skips. Coverage includes SQLite/PostgreSQL summary
semantics, null/negative/zero scores, ties, shared and unrelated ownership,
inactive rows, empty/error/canceled reads, a dense eight-workspace plan with one
base scan, joined backfill cancellation/history, demand sharing and existing
dashboard evidence/count boundaries. The first focused run retained one failed
assertion because its SQL fingerprint still matched the old country query. Its
replacement matches the shared query and still requires exactly one loader
across sixteen concurrent readers and a timer. Both owned PostgreSQL services
stopped cleanly. Fresh full checks and representative acceptance remain required.
