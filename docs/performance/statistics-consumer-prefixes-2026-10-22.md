# Bound statistics consumer transactions by evidence fanout

The populated release rehearsal accumulated statistics while all four consumers
repeatedly reached their 30-second database deadlines. A 5,000-result physical
batch shared by eight workspaces can update 40,000 latest-evidence rows in one
transaction. The physical batch threshold did not bound that expansion.

The consumer now commits original-event prefixes with at most 5,000 potential
latest-evidence rows. It counts valid workspace/key evidence and charges at
least one row for every original result, including legacy results. Duplicate
evidence is conservatively counted. The existing 5,000-result flush trigger
still applies. Eight-owner results therefore produce prefixes of 625 physical
checks; one-owner and legacy flushes of at most 5,000 results still use one
transaction. An indivisible event above the evidence limit runs alone, emits a
warning, and retains the same deadline. The PostgreSQL regression persists a real
5,001-owner event in its own transaction.

One 30-second context covers preparation and every transaction in a flush. The
stream path uses that same deadline for acknowledgement. A later prefix cannot
restart the clock. The existing 15-second flush interval, four-worker default,
producer policies, stream settings and queue capacity are unchanged.

Each successful prefix immediately consumes its original inputs and clears
their payload references. A failed suffix remains buffered. Original positions
survive deleted-route filtering, so progress counts original events rather than
inserted rows. Identity-free memory results from an earlier committed prefix
are never submitted again. Replay-only and deleted-only prefixes finish their
original inputs even when they create no new history.

Stream identities remain independent from decoded statistics: a malformed
payload whose discard acknowledgement failed can leave only an ID. Before any
flush writes or acknowledgements, every buffered statistic must have its stream
identity and a matching tracked ID. IDs absent from the remaining original
statistics are ready for acknowledgement. Successful acknowledgements remove
those IDs by identity, rather than by slicing an assumed positional prefix.
Failed acknowledgements retain an acknowledgement-only state, retried before
further persistence; a failed flush retries before reading more stream traffic.
A restart can replay both durable prefixes
and unfinished suffixes through the existing transactional event ledger.

Cancellation leaves stream entries pending. The final memory drain instead
retries its retained suffix, with the existing one-second backoff and a bounded
30-second context per attempt, until it is empty. It has no independent deadline
that could return success with volatile work remaining. The actual configured
Runtime shutdown deadline controls the result: an unfinished consumer keeps its
wait group active and makes shutdown fail. This does not make volatile fallback
data durable across a forced process exit.

`database.InsertProxyStatistics` retains its atomic contract. Every prefix still
commits replay identities, history, daily counters, latest evidence, reputation
requests and usage together. Its separate transaction-order correction is
documented in the [transaction-order report](statistics-transaction-order-2026-10-22.md).

## Verification

The focused regressions use disposable PostgreSQL and Redis. They cover a
committed prefix followed by a failed or canceled suffix, identity-free memory
retry, deleted original positions, payload release, shared preparation and
transaction deadlines, malformed interleaved IDs, acknowledgement-first retry,
invalid metadata, replay-only prefixes, deleted-only prefixes, cancellation
during prefix acknowledgement and worker restart. They assert exact history,
daily counts and usage in all eight workspaces, plus latest evidence and durable
reputation requests. A maintenance pass between prefix commits verifies that
the replay floor stops at the oldest pending suffix, and retired prefix events
cannot count again.

The final-memory regression observes an incomplete worker under two scaled
outer wait budgets, repairs the injected database failure and waits for every
retained result to persist. Cleanup must finish that worker before restoring
its global database and queue. SQLite/miniredis run the portable cases with
small database batches; true wide-owner and 5,000-result persistence proofs use
PostgreSQL because they exceed SQLite's SQL parameter capacity.

PostgreSQL/Redis normal and race checks passed in 32.590 and 46.041 seconds; the
portable run passed in 19.136 seconds with the large PostgreSQL-only cases
explicitly skipped. After the final database-order change, the focused prefix
normal and race checks passed again in 21.351 and 33.920 seconds. Their manifests
verify unchanged runtime, test and database-source hashes across both commands.

Run the broader race checks against disposable PostgreSQL and Redis:

```sh
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=32772 user=magpie_consumer_test password=magpie_consumer_test dbname=magpie_consumer_test sslmode=disable' \
MAGPIE_TEST_REDIS_URL='redis://127.0.0.1:32771/0' \
  go test -race ./internal/jobs/runtime -run 'Test(Statistics|CancelledStatistics|FailedAtomicAck|StreamMaintenance|AddProxyStatistic|ResolveStatistics)' -count=1 -v
```

Accepted commands, logs and source hashes are retained under
`.codex-tmp/release-remediation-20261020/statistics-consumer-20261022/` in the
workspace. PostgreSQL correctness fixtures use port 32772, PostgreSQL 17 Alpine,
two CPU slots, 3 GiB memory, 256 MiB shared buffers and 16 MiB work memory. Redis
uses the separate disposable service on port 32771. No production or restored
release database is used by these tests.

## Operation budget

The default checker dequeue, decode, check and requeue path gains zero
cryptographic operations, route-fingerprint calculations, JSON encodes, Redis
commands or database queries. Stored route hashes, plaintext queue defaults,
legacy payload readers and scheduling-only normal requeues are unchanged.
Evidence counting and identity sets run in the asynchronous consumer.

A successful 5,000-event stream flush still makes five acknowledgement Lua
calls, with either one owner or eight owners. The real-Redis regression warms
the script first and asserts that count; cold script loading retains the
existing `NOSCRIPT` fallback. Splitting database transactions does not add an
acknowledgement per transaction. A partial failure can acknowledge its durable
prefix separately, which may add a call at that failure boundary and prevents
unnecessary replay. Malformed-entry discard calls are unchanged.

The consumer still prepares route existence once per flush. Additional database
transactions and statements are confined to batches whose evidence fanout or
physical count exceeds the new bound. The representative transaction
measurements below quantify that cost and the shorter lock duration; they are not an application
soak or a production throughput guarantee. The coordinated validation driver
owns final combined Go checks and the full release rehearsal.

## Populated transaction comparison

The separate transaction fixture contains one million routes, eight million
ownership rows, eight million existing latest rows and one million rows each of
history, daily counters, replay identities and reputation requests. It restores
the original secondary indexes and validates its four foreign keys. PostgreSQL
17 runs with four CPU slots, 4 GiB memory, 128 MiB shared buffers and 4 MiB work
memory. Each worker checks 5,000 distinct existing routes shared by all eight
workspaces, producing 40,000 actual latest-row updates. Four workers use disjoint
routes and contend on the same eight usage rows. They share one 30-second
deadline. These are end-to-end `InsertProxyStatistics` measurements, including
SQL tracing, without application readers, projection workers or Redis.

The final-source comparison uses the same corrected database transaction order
for both physical prefix sizes:

| Workload | Physical events per transaction | Transactions | Traced SQL statements | Total elapsed | Longest transaction | Committed events |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One worker, three trials | 5,000 | 1 | 33 | 8.488–11.633 s | 11.633 s | 5,000 each |
| One worker, three trials | 625 | 8 | 88 | 5.061–6.134 s | 0.956 s | 5,000 each |
| Four workers | 5,000 | 4 | 132 | 27.630 s | 27.630 s | 20,000 |
| Four workers | 625 | 32 | 352 | 16.397 s | 2.418 s | 20,000 |

For 5,000 eight-owner checks, bounding the work adds seven atomic transactions
and 55 traced SQL statements: 0.0014 transactions and 0.011 traced statements per
physical check. Trace counts include savepoints but exclude driver-level
`BEGIN`/`COMMIT`; the seven additional transactions also require fourteen
transaction-boundary commands. Including those boundaries gives 35 versus 104
database commands per 5,000 checks, an amortized increase of 0.0138 per check.
The existing route-preparation lookup is outside this insert comparison and
remains once per flush. This extra consumer work is deliberate: it bounds the
latest-row fanout and shortens lock ownership while retaining durable progress
when a later prefix fails. No new statement runs inside the checker loop.

The comparison's original usage-before-latest order committed only two of four
workers before its deadline. The corrected order alone allowed the full-size
transactions to finish in this run, close to the same deadline; bounded prefixes
finished sooner and capped the longest transaction at 2.418 seconds. These
limited trials establish the local tradeoff, not latency distributions or the
capacity of the complete running service. The transaction test verifies exact
usage, active capacity, accepted history and latest-row counts after every
committed workload.

Accepted measurement evidence is
`statistics-transaction-order-20261022/million-final/cost-results.json` and
`million-final-cost.log` under the same workspace evidence root. The earlier
`million/` comparison is diagnostic evidence, not the final-source result.
