# Skip unchanged projection rows before conflict locking

The retained-session preflight failed on a 15-second projection-probe client
deadline after 9765.991 seconds of catch-up. Peak and last eligible matches
were 528 of 1024. The startup and failure observers each retained a client
timeout, and the backend failed its read-model drain. These remain failed
validation evidence. Recovery, smoke and the load gate did not start.

The projection already uses a null-safe conflict predicate to avoid rewriting
unchanged content. PostgreSQL still locks a conflicting row before evaluating
that predicate. Repeated invalidations can therefore lock rows whose complete
projected content is already current.

The query now excludes those rows before entering the insert. It compares the
same 26 source-derived content fields, including nullable health, reputation
and IP values. Missing and changed rows still enter the upsert, whose existing
conflict predicate remains in place after acquiring the row lock. Current
ownership, lifecycle, checker generations, protocol overrides, history and
metadata inputs are unchanged. An unchanged row keeps its timestamps.

The extra projection lookup adds work when rows have changed. It is inside the
existing statement, with no additional database command. The reason for it is
to avoid conflict locks for unchanged rows, rather than to assume every refresh
is cheaper. The ordinary pair ceiling, statements per invocation, timer,
child timeout and adaptive retry behavior are unchanged.

## Stopped-fixture measurement

One comparison ran each query once on the stopped million-route fixture with
eight workspaces and all existing indexes and resource limits. It selected
625 spread-out physical routes, or 5000 owner pairs. Both transactions rolled
back to the same complete 5000-row fingerprint. Physical WAL, page and index
effects of the transactions remain.

| Observation | Original | Filter before insert |
| --- | ---: | ---: |
| Conflicting rows entering the upsert | 5000 | 2440 |
| Rows rejected by the conflict predicate | 2560 | 0 |
| Existing rows with changed content | 2440 | 2440 |
| Inserts | 0 | 0 |
| SQL execution time | 17.663 s | 11.338 s |
| Shared read blocks | 75004 | 74438 |
| WAL records | 63871 | 61342 |
| WAL full-page images | 47058 | 415 |
| WAL bytes | 240414289 | 7191807 |

The original ran first. The later query inherited warmer pages, physical
effects from the rolled-back update and a different full-page-image state.
Its lower elapsed time and WAL bytes do not establish a causal reduction or
live capacity. Shared read blocks are not physical disk bytes. The concrete
result is that the candidate omitted 2560 unchanged rows from conflict
processing while retaining the same changed-row count.

Raw queries, plans, rollback fingerprints and the two script syntax rejections
that occurred before database execution are retained under
.codex-tmp/release-remediation-20261020/takeover-20261025/rehearsal-session-failure-20261028/projection-noop-filter/.
The reputation-claim plan found an empty stopped queue, and the failure-streak
query already used an index. Neither justified a production change.

## Validation and checker cost

PostgreSQL regressions repair each of the 26 content fields separately,
including NULL and false values. They also require an unchanged refresh to
finish while another transaction holds its projection row lock. A changed row
must still wait, fail on cancellation without committing content, and succeed
on retry after the lock is released.

All 45 selected groups passed without skips, normal in 53.561 seconds and race
in 62.551 seconds. Both owned PostgreSQL test services stopped cleanly. These
are functional test durations, not capacity measurements.

The focused suite also covers ownership changes, concurrent invalidations,
atomic deadlines, ambiguous commits, no-replay retries, source work and clean
drain behavior. Fresh full checks and the unchanged representative recovery,
smoke and complete 25-hour load gate remain mandatory.

No encryption, decryption, nonce generation, route hash, JSON encoding, Redis
command, credential load or database query is added to a default physical
proxy check. The plaintext Redis queue default and stable encryption key remain.
