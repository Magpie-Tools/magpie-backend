# Projection width after slow successful commits

The 28 October startup-inventory resume exhausted its unchanged six-hour
catch-up budget at 16:14:44 UTC. Peak and last eligible matches were 1016/1024.
Startup observation retained 116 PostgreSQL samples and three client timeouts;
the shutdown drain exited 1. No recovery, smoke or load phase started.

Retained logs contain 135 projection failures: 119 at 5000 pairs, 14 at 2500 and
two at 1250. Twenty-eight full-width failures occurred on a newly discovered
owner batch; the others do not establish why a particular statement failed.
Concurrent DataFileRead and WAL waits establish pressure, not a single cause.

One stopped original 1250-pair upsert completed in 1119.174 ms after earlier
diagnostics had warmed the fixture. Its input subtree took 39.176 ms; the root
updated 1250 conflicting rows, with 6178 shared reads, 5676 dirtied buffers,
31686 WAL records, 5455 full-page images and 31167515 WAL bytes. Every requested
projection column, including timestamps, matched after rollback. WAL, buffer,
index and rollback effects remain. This is no causal write-cost or capacity
comparison. Smaller generic upsert plans also use merge joins for latest
evidence; the separate DISTINCT ON proposal remains unapplied.

The stopped cohort breakdown also found one route whose latest result was dead
for all eight owners despite matching projection timestamps. Its retained
request duration was 400 ms under the unchanged 2000 ms/no-retry profile, with
no saved response. The transport cause is unknown. Other cohort rows had stale
projection timestamps after rechecks. Neither condition earns acceptance.

## Successful-commit backoff

Previously a successful full-width statement taking almost the complete child
budget left the next width unchanged. The batch now halves its next width when
that commit takes more than half the existing timeout. Exactly half does not
trigger backoff. Only a full-width commit can reduce the window; a short tail,
invalid timing or one-pair statement cannot reduce it further.

The successful prefix remains committed and advances its cursor. No failed
interval is created. Recognized live-parent child deadlines, cancellation,
unknown errors and ambiguous commits retain their handling. Two consecutive
full-width commits taking at most a quarter of the child budget still permit
doubling toward 5000. Learning remains local to the current owner batch; a new
batch starts at ordinary width.

Only readModelProjectionWindow.committed and its comment change production
source. The worker reuses its existing duration measurement. All projection,
ownership, evidence, no-op and conflict SQL remain byte-identical, as do every
index, resource, PostgreSQL setting, stable key and plaintext queue default.
The 5000-pair ceiling, four statements per ordinary invocation, five-second
timer and 30-second child context remain unchanged. No default physical-check
crypto, decryption, nonce, route-hash, JSON, Redis, credential/database operation
or clock read is added.

Smaller statements add background round trips. In the controlled PostgreSQL
eight-owner fixture, one slow four-pair commit is followed by two two-pair
commits: three projection statements/table checks and one owner lookup, versus
two four-pair statements/table checks and one lookup without backoff. The
fixture verifies complete committed progress and unchanged authoritative data;
its delay measures no representative throughput.

Forty-nine selected groups passed normally in 57.220s and under race in
65.459s, without skips. Both exact owned PostgreSQL services stopped cleanly,
exit 0. They include successful backoff boundaries, growth/fresh-batch behavior,
real committed prefix/drain checks and retained atomic deadline, ambiguous
commit, no-op content, ownership, cancellation and source-only regressions on
the current dependency graph.

All earlier failures and rejected ownership proposals remain in private
evidence. Fresh full source checks, representative recovery, corrected smoke
and the complete unchanged 25-hour load gate with zero observer errors remain
mandatory. No live improvement or readiness pass is claimed.
