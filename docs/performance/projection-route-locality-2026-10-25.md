# Shared-route projection locality

The 25 October takeover passed all twenty preparation stages, including the
stopped read-model repair, then failed its strict catch-up probe. Its last
successful sample matched 75 of 1,024 current projections despite empty due and
statistics queues. The maximum observed match was 280. The final client command
exceeded its 15-second bound before the six-hour preparation budget expired;
application shutdown also failed to drain the read models. These failures remain
in `.codex-tmp/release-remediation-20261020/main-validation-subprefix-takeover-20261025/resume/`.

The existing executed plans show repeated metadata reads when expanded owner
pairs are ordered by workspace. With eight owners, eight 5,000-pair statements
each look up the same 5,000 physical routes and their reputations. The candidate
orders discovered pairs by physical route, then workspace. Each statement can
share its materialized metadata across owners. The projection SQL still orders
its output by workspace and route before acquiring row locks. Pair limits,
timeouts, retry intervals, contiguous cursor accounting, evidence validation,
ownership checks, indexes and the resource profile are unchanged. Explicit repair
retains its existing workspace/route keyset enumeration.

A focused read-only diagnostic used the stopped failed million-route fixture
and the unchanged production projection's SELECT. Both orders covered the same
40,000 owner pairs in eight 5,000-pair statements. Executed plans counted 40,000
route lookups and 40,000 reputation lookups in workspace order, compared with
5,000 of each in route order. Shared read buffers were 285,331 and 105,966;
execution totals were 14,204.574 ms and 3,587.475 ms respectively. Workspace order
ran first. Cache, I/O and order effects prevent causal timing attribution. These
reads omit the upsert and its index/WAL cost; they establish neither write
throughput nor live catch-up capacity. Complete plans are retained under
`takeover-20261025/preflight-failure-20261025/locality-read-only/`.

The default physical checker loop adds zero cryptographic operations, route
hashes, JSON encodes, Redis commands, credential loads or database queries. The
asynchronous updater uses the same owner-discovery query count, table checks,
upsert count and source lookup. Only discovery's sort keys change; the SQL's
existing distinct-metadata lookup handles the grouped routes. The opt-in cost
test now advances evidence for the exact mixed-workspace prefix it measures.

Focused regressions passed all 20 top-level cases without skips, normally in
14.963 seconds and under the race detector in 22.433 seconds. They include the
new shared-owner locality/retained-suffix proof and existing deadline, rollback,
after-commit retry, ownership and source-drain cases. The owned PostgreSQL 17
service stopped cleanly, exit 0, with no OOM or restart. Full required checks
must still pass on the final source. Acceptance still requires strict catch-up, a clean coordinated recovery
into fresh volumes, corrected smoke and all 25 hours of the unchanged load gate.
The read-only diagnostic is not load credit. Item 4 remains deferred.
