# Projection width after a child deadline

The dashboard-inventory resume failed at 05:33:37 UTC on 27 October after
11532.588 seconds of catch-up. Peak and last eligible matching were 808/1024;
the final projection SQL client exceeded 15 seconds with an empty queue. Startup
diagnostics retained 118 PostgreSQL samples and two collection errors. The
failure observer had zero errors. Application drain exited 1 without OOM or
restart. No readiness dashboard request, recovery, smoke or load gate started.

The application logged 55 projection failures. The median failed projection
stage used its 30-second budget. Logs include 5000-pair deadline attempts at
cursors 5000 and 10000 of retained 40000-pair owner batches. The failure observer
captured a projection statement active for 20.203 seconds in DataFileRead, with
no blocking PID. These observations do not establish one cause of every timeout.

## Bounded write diagnosis

On stopped application data, one read-only production projection SELECT covered
5000 pairs: 625 nearby physical routes shared by eight workspaces. It took
1.043 seconds. A subsequent actual production upsert for the same pairs updated
all 5000 existing rows in 3.985 seconds, generating 127737 WAL records, 21566
full-page images and 123546899 WAL bytes. A complete row fingerprint matched
before and after rollback. Logical rollback retains physical WAL, page and
index effects.

The SELECT ran first and warmed data. Both measurements omitted live checker,
statistics and reputation writers. Their timings cannot isolate a causal write
cost or establish live throughput. They show that a read-only projection receipt
does not cover the update path. The old paired benchmark framework was not
repeated; its incomplete semantic verification and candidate preparation remain
incomplete evidence.

## Batch-local width control

Previously, finishing a failed interval immediately restored the ordinary
statement width, even when another interval in the same retained batch then
hit the same child deadline. A recognized child projection deadline with a live
parent still halves the attempted interval. Its reduced width now also limits
the remaining owner batch until two consecutive full-width commits each take
at most one quarter of the configured child timeout. Width then doubles up to
the ordinary configured ceiling. Short tails, slow commits, errors and invalid
timing cannot supply that growth evidence. Growth is a scheduling rule, not a
guarantee about the next statement's latency.

Only completed pairs advance the cursor. Unknown SQL errors, source-only
failures, canceled parents and one-pair failures keep their existing handling.
The failed interval remains contiguous until every pair commits. The learned
width belongs to that retained batch and is released with it; a new owner batch
starts normally. Fast recovery from a one-pair interval grows exponentially
rather than leaving the whole batch at one pair per statement.

The default 5000-pair ceiling, four statements per ordinary invocation,
five-second timer and 30-second child context are unchanged. Projection SQL,
owner discovery, source queries, indexes, resources and checker semantics are
unchanged. A byte comparison verifies the SQL/query regions against the failed
source. No cipher, route hash, JSON encode, Redis command, credential load or
database query was added to the default physical checker. The worker uses its
existing elapsed-time measurement; the new state is in memory per retained
batch.

Smaller statements can add database round trips. In the tiny real-PostgreSQL
single-deadline regression, projection attempts and table checks increased from
six to seven, with one owner lookup. Its uninterrupted reference remains four
projections and four table checks. In a controlled slow-commit case, one failed
two-pair statement is followed by eight one-pair commits. That artificial delay
tests scheduling and durable completion; it measures no representative capacity
or benefit from retries.

Forty-two selected functional groups passed normally in 28.091 seconds and
under race in 34.878 seconds without skips. They cover growth boundaries,
consecutive timing evidence, operator ceilings, fast recovery from one pair,
actual PostgreSQL cancellation/atomic rollback, ambiguous commits, source-only
retries, concurrent invalidations and ownership, unchanged timestamps, command
counts and clean drains. The first focused run retained a failed assertion that
expected the old source-only retry to finish after three calls. The updated test
requires cursor six after those calls, then full completion after the next;
its no-replay assertion remains. Both owned PostgreSQL services stopped cleanly.
The existing opt-in populated-cost benchmark remains outside the selected
functional scope; its initial skipped receipt is retained.

All failures and raw measurements remain in private remediation evidence.
Fresh full checks, current-source recovery, corrected smoke and the complete
25-hour load gate are still required. No live benefit or acceptance is claimed.
