# Backend longevity fixes, 13 October 2026

The fixes bound retained state and recover background work after dependency failures.
Regressions run in the normal Go suite.

| Area | Change |
| --- | --- |
| Statistics ledger | Retire identities behind a durable per-stream replay floor; reject retired and duplicate results. |
| Redis streams | Delete and acknowledge committed results in one script; retry failures and recreate lost consumer groups. |
| Queue recovery | Production startup launches a ten-second marker observer. Missing state rebuilds proxy/source queues under shared leadership; readiness is 503 after detection until reconstruction completes. |
| Redis startup | Retry leadership, configuration subscriptions, checker settings, and stream initialization. |
| Daily counts | Compact older days into lifetime totals while preserving dashboard counts and preventing historical backfill. |
| Orphan cleanup | Drain all counter tables in batches of 5,000 route IDs; retry interrupted 45-second passes after one minute. |
| Partitions | Maintain current/future months at runtime, move DEFAULT rows atomically, and resolve DEFAULT existence by parent relation ID. |
| Caches and metrics | Bound DNS/robots caches with expiration and LRU eviction; collapse unknown HTTP paths and methods into fixed metric labels. |
| Rotators | Reload valid TLS certificate replacements, close client/upstream sockets on stop, and limit tunnel clients. |
| Other retained state | Expire abandoned email, remove drained traffic counters from earlier months, and refresh the public IP periodically. |

Stop old backend instances and run `--migrate-only` before upgrading. The migration
adds replay-floor storage, a sortable ledger key, retention indexes, and lifetime
totals. Large DEFAULT-partition moves can extend the migration window. Keep
PostgreSQL/Redis backups coordinated and `PROXY_ENCRYPTION_KEY` stable. An old
pending stream event delays ledger retirement; monitor pending age, the replay
floor, and maintenance success/failure metrics.

The default checker loop adds zero cryptographic operations, JSON encodes, Redis
commands, or database queries. Plaintext payloads retain their stored hashes and
normal requeues update scheduling only. Queue encryption remains opt-in.
Asynchronous statistics ingestion adds one floor-initialization statement per
batch, plus floor locking and a sortable-key index in the existing ledger insert.
That is 0.0002 additional statements per result at 5,000 results per batch, needed
to reject delayed replays after identities are retired.

Local PostgreSQL 17 benchmarks used 5,000-event batches and three three-second
runs. Single-worker median ledger acceptance changed from 47,366 to 38,943
events/second, with median batch time rising from 105.6 to 128.4 ms, about 22%.
Four-worker rates overlapped: baseline 102,087 to 166,333 and new 112,506 to 116,107
events/second. Allocations rose from about 19.4 to 27.2 MB per batch. These measure
ledger acceptance alone, not full checker throughput.

Independent verification passed full unit/race suites, vet, build, PostgreSQL 17.9
and Redis 7.4.8 integrations, targeted integration races, documentation build, and
Compose validation. Normal and race-instrumented application binaries each
recovered from two queue losses, with readiness transitioning from 503 to 200.
Three passes deleting 10,000 routes each left zero orphan counters in every table.
Interruption/resume, cross-schema DEFAULT recovery, compaction, replay, cache
churn, certificate renewal, tunnel shutdown, and abandoned-email tests passed.
The opt-in 200,000-rule alert cadence test was skipped. A months-long soak remains
unperformed.
