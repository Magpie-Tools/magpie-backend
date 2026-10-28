# Ordered rounds for pending projection work

The preceding route-locality source passed full preparation, then failed its
six-hour catch-up limit on 26 October. Matching reached 1,008 of 1,024 sampled
owner pairs, then fell behind as another physical check cycle arrived. Startup
diagnostics also retained one client-timeout error, and shutdown failed its
read-model drain. These are failures, preserved under
`.codex-tmp/release-remediation-20261020/main-validation-route-locality-takeover-20261025/resume/`.

Grouping owners within a statement reduced repeated metadata work, but selecting
the next physical IDs from an unordered dirty map still scatters index/heap
access across the million-route inventory. The candidate closes one dirty set,
sorts its physical IDs once and consumes bounded prefixes. Owner pairs remain
grouped by route; projection writes retain workspace/route lock order. Newly
committed invalidations for unseen IDs coalesce into their future statement,
which reads current authoritative data. In-flight or already-popped IDs and
new routes remain in the dirty map for the next round. New arrivals cannot
displace an unseen suffix. Owner-discovery failures requeue their selected IDs.
The existing owner-pair cursor, deadline subdivision, source notification and
shutdown checks retain pending work. Explicit stopped repair is unchanged.

The candidate adds an eight-byte ID slice per pending physical route, plus Go
slice/sort overhead and the existing dirty map. A million pending IDs need about
8 MB for the slice. There is one O(n log n) sort per closed round under the
existing dirty mutex. Notifications binary-search its unseen sorted suffix to
avoid repeated SQL for work it will already cover. A bounded pop copies only
its selected IDs so a retained owner batch cannot retain an exhausted large
backing array. Sorting and lookup CPU cost must be measured alongside capacity;
this is not a claim that queue memory or invalidation latency is constant.

A focused write diagnostic used the stopped failed million-route fixture, its
unchanged production upsert, eight owners, 5,000 changed owner rows per statement
and all 23 projection indexes. Each transaction advanced only its selected
current HTTP evidence by one second, measured the upsert and rolled back. Both
latest-evidence and projection-content hashes matched before and after rollback.
The sparse cohort used 625 physical IDs spanning the inventory; the nearby cohort
used 625 consecutive IDs. Sparse ran first. Different cohorts, cache/checkpoint
state and execution order prevent causal throughput attribution.

| Executed root-plan measurement | Sparse IDs | Nearby IDs |
| --- | ---: | ---: |
| Changed owner pairs | 5,000 | 5,000 |
| Unchanged conflicts skipped | 0 | 0 |
| Execution time (ms) | 22,633.934 | 5,849.825 |
| Shared read buffers | 122,955 | 33,858 |
| Shared dirtied buffers | 100,731 | 29,374 |
| Shared written buffers | 87,317 | 24,202 |
| WAL bytes | 480,850,843 | 152,442,559 |

This bounded diagnostic motivates preserving physical locality. It does not
establish committed live throughput, concurrent catch-up, full inventory
correctness or sustained stability. Exact private evidence is under
`takeover-20261025/route-locality-failure-20261026/page-locality-write-diagnostic/`.
The earlier paired benchmark framework was not repeated and remains incomplete.

The physical checker loop adds zero cryptographic operations, route hashes,
JSON encodes, Redis commands, credential loads or database calls. Notifications
remain memory-only after statistics commit. There is no new SQL operation per
projection batch; owner discovery, pair limits, upserts, source lookup, all
indexes/resources and every existing validation bound remain unchanged. Unseen
notifications coalesce, while in-flight invalidations remain conservative
retries as before.

All 25 focused cases passed without skips: normal in 18.552 seconds and race
in 24.181 seconds. Proofs cover sorted/fair closed rounds, owner-discovery
requeues, concurrent notifications, fresh committed evidence for unseen routes,
all existing retained-prefix/rollback/ownership/source cases and a million-ID
round. The million-ID case, including 995,000 coalesced notifications and 200
bounded pops, took 347.113 ms normally and 1.418 seconds under race. These are
local queue costs, not live application throughput measurements. Both owned
PostgreSQL test services stopped cleanly, exit 0, without OOM or restart.

All required full-source checks must still pass before acceptance. A clean coordinated recovery, corrected smoke and the unchanged
25-hour load gate remain mandatory. Repair, diagnostics and catch-up provide no
load credit. The observer error and failed application drain cannot be repaired
retroactively. Item 4 remains deferred.
