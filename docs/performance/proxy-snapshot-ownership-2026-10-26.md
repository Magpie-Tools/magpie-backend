# Scoped ownership for alive-count snapshots

The 26 October import-usage validation failed its catch-up probe at 13:10 UTC.
Its last valid sample matched 936/1024 required projections. The queue had
drained, but the final SQL client exceeded 15 seconds and the application drain
later exited 1. The startup observer also retained one Docker-stats timeout.
These remain failed acceptance evidence.

Concurrent activity included snapshot counts lasting 158.713 and 85.928 seconds,
DataFileRead and WAL waits, and one projection batch whose owner discovery used
18.412 seconds of its unchanged 30-second child context. These observations do
not establish a single cause of every failure.

The snapshot query first identifies distinct workspace/route pairs with current
alive evidence. With dense evidence, PostgreSQL underestimated that set and
performed an ownership primary-key lookup for each pair. The query now scopes
active ownership to the requested workspaces in a subquery. A PostgreSQL
`OFFSET 0` barrier prevents pushing each evidence pair into that subquery. The
full-fixture plan joins one ownership scan to the evidence set. Other dialects
retain ordinary subquery planning.

Current configuration and per-protocol overrides, nonempty evidence keys,
generation-zero compatibility, dirty-workspace exclusion, active ownership and
distinct physical-route counting are unchanged. The operation remains one SQL
statement. No schema, index, resource, isolation or runtime deadline changed.
The default physical checker adds zero cryptographic operations, route hashes,
JSON encodes, Redis commands, credential loads or database queries.

## One full-fixture read-only comparison

Both count queries ran once in one repeatable-read, read-only transaction on the
stopped million-route/eight-workspace application fixture, with PostgreSQL still
limited to 4 CPUs and 4 GiB. An execution-time assertion verified identical
counts for all eight workspaces. The transaction rolled back; neither query
wrote application data or WAL.

| Measurement | Original join | Scoped ownership |
| --- | ---: | ---: |
| Ownership primary-key lookups | 7,980,026 | 0 |
| Scoped ownership scans | 0 | 1 |
| Root shared read blocks | 1,652,123 | 501,671 |
| Root shared hit blocks | 30,606,032 | 15,214 |
| Root temporary read blocks | 50,763 | 155,139 |
| Root temporary written blocks | 50,874 | 155,294 |
| Count subplan time | 36.494 s | 12.340 s |

Shared reads are buffer-accounting operations, not physical disk-byte counts.
The original query ran first; cache state and stopped application conditions
prevent a causal claim about live speedup or capacity. The scoped join uses more
temporary-file I/O. Its reason is to avoid millions of repeated ownership
probes when current evidence is dense, while preserving the exact counts.

Three focused groups passed normally in 8.216 seconds and under race in 12.153
seconds, without skips. They cover current/obsolete keys, per-route protocol
overrides, multiple alive protocols, dirty and legacy generations, inactive and
orphaned ownership, and sparse/dense inventories with one or eight requested
workspaces. The dense PostgreSQL regression checks counts and bounded ownership
scan loops; the sparse regression retains its requested-owner row bounds. The
owned test PostgreSQL stopped cleanly, exit 0, without OOM or restart.

Fresh full validation is still required. No representative recovery, smoke or
required 25-hour gate pass is established.
