# Projection deadline subprefix candidate, 25 October 2026

Status: applied candidate; formatting, focused compilation, targeted normal/race
regressions and targeted vet passed. Paired production-workload observations are recorded below. Their diagnostic
limitations prevent an accepted performance benefit claim. Full-source and
end-to-end validation are authorized; no capacity, recovery or readiness pass is claimed.
The original private patch, preparation and after-snapshots remain immutable.

The preceding fixed-source attempt ended at its original six-hour deadline with
incomplete projections, and the backend exited with a failed drain. Its observer
collection failure and all ingestion/projection deadline failures remain
failure evidence. Root authorized this candidate for focused testing only.

Saved post-drain evidence shows continued matching-projection progress alongside
repeated deadline warnings. It does not expose retained prefix identities or
prove one permanently stuck cursor. The completed earlier four-prefix diagnostic
also had little first-context margin and substantial read/WAL work. Those
measurements are retained; this candidate has no representative measurement result.

The candidate implementation keeps owner discovery, SQL text, indexes, defaults, the
five-second timer, four-statement ordinary-pass limit and 30-second child budget.
It adds a failed-interval boundary and retry width to the existing retained
owner-pair batch. Only a known projection operation deadline from an actually
expired child, with a live parent context, may halve a later statement inside
that interval. The failing invocation returns immediately. Successful smaller
statements advance the existing contiguous committed cursor. Completing that
interval restores the ordinary width. No pair can be skipped.

Owner discovery has not selected projection pairs, so its failures only retain
the existing physical dirty IDs. A source-lookup failure keeps the completed
projection cursor. Neither stage creates a split interval. Parent cancellation,
permanent errors and one-pair failures remain pending. Unknown driver errors are
not reclassified by string matching to obtain a successful test.

A deadline after an autocommit response can leave the cursor uncertain even
though the statement committed. The unchanged conflict guard must preserve its
row timestamps when the smaller retries read already committed content. Newly
arriving invalidations remain in the existing dirty set. Source invalidations
still follow completion of the entire owner batch. The final drain must continue
to report unresolved work rather than claim success.

## Operation accounting

The checker dequeue/check/requeue loop receives zero new cryptographic calls,
route hashes, JSON encodes, Redis commands, credential loads or database calls.
The notification into the asynchronous read-model queue remains memory-only.
The ordinary projection path has the same owner lookup, table checks, upserts
and final source lookup. The candidate adds bounded in-memory bookkeeping,
lock/timestamp operations and numeric fields on existing debug/warning events.
It also adds a warning for an already failing source post-query cancellation.

Exceptional retries cost more SQL round trips. One timed-out width `m`, followed
by two successful half-width statements covering the same interval, has three
upsert attempts and three projection table checks. A same-sized retry after one
failure would have two of each; an uninterrupted statement would have one of
each. Thus the analytic extra cost is one upsert plus one table check compared
with one failed full-width retry, or two of each compared with no failure.
Owner discovery and source invalidation counts are unchanged for that batch.
These are analytic counts, not measured throughput results.

Repeated subdivision can reach one pair per statement. At that width it may
need up to `m` successful statements for the interval; repeated one-pair failures
remain bounded by the existing outer lifecycle/validation limits. No claim is
made that this tradeoff improves aggregate throughput, WAL, latency or recovery.

The real PostgreSQL operation-count case passed normally and under race detection.
It used one route, eight owners, a two-pair test budget and an injected child
deadline. The attempted widths were `2,1,1,2,2,2`: six upsert attempts, six
projection table checks and one owner discovery. The uninterrupted existing
reference case passed with four upserts and four projection table checks.
Fixture DDL and verification reads are outside those counts. These are small
controlled regression results, not representative throughput measurements.

## Focused validation completed

Root verified all 1,117 live/saved baseline source hashes before applying the
reviewed nine-file candidate. Four changed Go files were formatted. Focused
database test compilation passed in 3.977 seconds.
The selected tests across database, proxy queue and runtime packages passed
normally in 34.459 seconds and with race detection in
53.522 seconds. Each run passed all 56 top-level tests,
including all 17 new tests, with zero skips. Targeted vet passed in
0.516 seconds; repository whitespace checks passed.

The new proofs cover ordinary widths, contiguous and odd interval boundaries,
repeated deadlines, parent cancellation, permanent-error races, one-pair failure,
new invalidations/ownership, owner/source deadlines, source-only retry and the
four-statement pass limit. Real trigger-induced deadlines cancel a statement
after earlier rows have been modified; rollback leaves no partial committed
rows, and later smaller statements commit independently. An after-commit
cancellation proof retains the uncertain cursor and verifies unchanged row
timestamps on retries. Callback-based cases establish coordinator/cancellation
semantics; they are not claims about server query execution.

Existing regressions also passed for retained-prefix progress and shutdown,
consumer/persistence atomicity, deadline sharing, acknowledgement/replay and
default plaintext payload/hash/Redis-command behavior. The checker hot loop
received no new cryptographic, hash, JSON, Redis or database operation.

Tests used fresh owned PostgreSQL 17 container `2884e9f66f72` with
2 CPU, 2 GiB RAM, a 4 GiB combined RAM/swap limit, tmpfs data and a loopback-only
port. It stopped cleanly with exit 0, no OOM, no restarts and no remaining server
PID. Test-owned miniredis instances supplied the Redis fixtures; retained source
services and their volumes/networks were not used. Logs, exact command timings,
source hashes and lifecycle evidence are under
`.codex-tmp/release-remediation-20261020/subprefix-validation-20261025/`.

These results establish focused semantic regressions only. The original attempt's
observer error, catch-up deadline, ingestion/projection timeouts and failed
application drain remain failures. No full combined validation or representative
measurement was run for this candidate.

## Required evidence before adoption

Representative acceptance still requires fresh guarded copies of the chosen
million-route/eight-workspace fixture with its original indexes, resources,
plaintext queue and stable encryption key. Capture full contiguous cursor
progress, SQL attempts, retry/deadline cost, complete logs/telemetry, WAL/growth
and simultaneous checker/persisted-result progress. Do not manufacture cache
conditions or reduce the gate to obtain a pass.

If adopted, run the fresh full Go normal/race suites, vet, build, vulnerability
scan and relevant distribution/docs/startup checks on the final source. Use only
local test images, then complete clean coordinated recovery, corrected smoke and
the unchanged 30-minute read, 30-minute write and 24-hour mixed gate. The original
observer collection failure is not repaired by any result from this candidate.
Item 4 remains deferred; there is no release artifact, commit, push or deployment.


## Takeover measurements and full-validation decision

The baseline arm used a new verified 1,734-file, 29,928,291,730-byte physical copy with the original catalog and resource limits. It recorded 40000 committed owner pairs, 8 upsert attempts and 0 statement errors in 98.781 seconds of ordinary production work. Overall diagnostic status is `incomplete_concurrent_diagnostic`. Its complete raw evidence remains retained.

The candidate arm did not start its projection workload. The existing dashboard cold preparation exhausted its unchanged 30-second deadline. The spawned dashboard process ended with exit 1, and its private clone stopped cleanly. This is failed preparation evidence, not a projection measurement or a demonstrated regression caused by the retry fix.

The post-workload semantic verifier has a separate three-second deadline. Its timeout closes the pinned diagnostic connection and prevents complete verification and final counter capture. Such an arm remains incomplete. Recorded successful SQL plans and cursor progress do not substitute for durable semantic verification, natural fallback benefit, sustained capacity or the final gates. Sequential arm order and cache/background effects prevent causal timing attribution.

The takeover agent approved this frozen candidate for fresh full-source and application validation under the user request. Acceptance still requires strict million-route catch-up, clean coordinated recovery, corrected smoke and the unchanged 25-hour gate. No diagnostic failure is counted as a pass. See `.codex-tmp/release-remediation-20261020/takeover-20261025/` for the assessment, decision and exact final manifests. Item 4 remains deferred.
