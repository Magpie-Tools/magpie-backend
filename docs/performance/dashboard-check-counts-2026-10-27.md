# Dashboard check counts through one ownership join

The projection-window resume failed at 12:46:49 UTC on 27 October on the
first dashboard request: HTTP 500 after 30002 ms and client
`RemoteDisconnected`. Catch-up sampling had settled at 1024/1024 after
14030.173 seconds. Startup diagnostics retained 118 PostgreSQL samples and two
client timeouts; failure diagnostics had no errors. The backend stopped cleanly,
exit 0. This remains failed preflight, with no recovery, smoke or load credit.

The application reported a pre-aggregated check-count timeout. On stopped full
data, a read-only cold loader took 24.039 seconds. Its daily/retained counter
query took 10.303 seconds, followed by 9.187 seconds for raw boundary counts.
Other concurrent dashboard reads also consumed substantial time. This does
not establish a single cause for the live timeout.

## Query change

Previously, the two count statements each joined current workspace ownership.
The new statement joins it once to a union of closed daily counters, retained
totals and raw boundary results. Explicit BIGINT and BOOLEAN constants keep the
union branches compatible with PostgreSQL parallel append. Retained rows supply
weighted counters; raw results supply nullable boundary flags. Counting a flag
counts successful and unsuccessful attempts. COALESCE preserves legacy NULL
results as attempts too.

The current UTC day, partial cutoff day and complete middle days keep their
previous treatment. Raw boundary rows retain the same cutoff and
zero-contribution predicates. Current ownership still includes active, paused
and archived management; shared routes count independently for each owner.
Retained totals survive history compaction. Query errors return no partial
counters, and cancellation, fallback, delayed backfill and joined maintenance
keep their existing behavior. The two-second backfill delay and 30-second
dashboard budget are unchanged.

This changes the dashboard count read from two SQL commands to one. It adds
no cipher, route hash, JSON encode, Redis command, credential lookup or database
call to the physical checker. Projection SQL, pair/invocation limits, timer and
child deadline are unchanged. All indexes, resource limits, PostgreSQL settings,
the stable encryption key and plaintext queue default remain unchanged.

## Bounded evidence and rejected proposals

Several stopped-data alternatives were rejected before source application:

- A materialized shared-ownership proposal returned equal counts but took
  17.292 seconds versus 13.374 seconds for the original sequential pair.
- Per-route lateral ranges, split boundary aggregates and an untyped event
  union each hit the separate 120-second diagnostic statement limit. Their
  equivalence assertions did not complete.
- An untyped result-flag union returned equal counts in 17.795 seconds, versus
  12.974 seconds for its original pair. Its plan was serial. Explicit integer
  casts restored a parallel plan with one ownership scan.
- A combined EXPLAIN containing the originals and typed proposal failed to
  resize PostgreSQL shared memory. That failure is preserved. Resource limits
  were not increased.

The final comparison executed the original statements separately and the typed
proposal once in the same repeatable-read, read-only snapshot. It asserted equal
total and weekly counts of 13,539,907. The candidate subplan took 10.546 seconds
and retained parallel scans with one ownership scan. It recorded 2,277,866 shared
read blocks and 50,368/50,432 temporary read/write blocks. Earlier original plans
recorded two ownership scans, 2,542,773 shared reads and 49,052/49,188 temporary
read/write blocks in total. Those executions had different cache states and
order; shared reads are not physical disk bytes. The temporary-I/O cost remains.

A separate read-only whole-loader probe through the exact proposed Go source
took 22.674 seconds, with its one count statement taking 18.194 seconds. The
earlier probe took 24.039 seconds. These separate stopped-data probes prohibit
optional backfill writes. They establish neither a causal live speedup nor cold
readiness, capacity or recovery. The earlier incomplete paired projection
benchmark framework was not repeated.

Twenty-two focused groups passed normally in 26.395 seconds and under race in
31.576 seconds without skips. They cover SQLite/PostgreSQL semantics, timezone
and exact cutoff boundaries, true/false/NULL results, current/shared/transferred
ownership, inactive management, retained counters, errors and cancellation,
one actual ownership scan, dashboard cache demand, joined backfill, retained
history and existing inventory behavior. The first focused run failed stale
fixtures missing compaction timestamps and result columns, plus the old
two-command assertion. Its source and receipt remain preserved. The corrected
fixtures retain exact count and zero-contribution-predicate comparisons. Both
owned test databases stopped cleanly.

External alert UI/test edits and an updating-guide edit during the failed run
were preserved. The current frontend passed a fresh build, all 449 browser tests
and a local image build. Those checks do not substitute for backend preparation
or any acceptance gate.

Raw failures, plans, comparisons and current-source proof remain private
remediation evidence. Fresh full checks, representative recovery, corrected
smoke and all 25 load hours are still required. Item 4 remains deferred.
