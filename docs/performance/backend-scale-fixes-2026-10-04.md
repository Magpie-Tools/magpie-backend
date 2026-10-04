# Backend scale fixes

This change addresses the eleven findings in
`backend-scale-review-2026-10-04.md`. Measurements below are local component
measurements on an Intel Core i7-12700H, with PostgreSQL 17 and Redis 7 in
disposable containers. They do not establish production capacity.

The independent verification found two remaining lease races and a statistics
shutdown loop. The follow-up fixes and validation are recorded in
[the lease follow-up](backend-scale-lease-followup-2026-10-04.md).

| Finding | Change and regression coverage |
| --- | --- |
| Pending statistics repeat | Advance the pending cursor after every page and flush partial recovery batches. Actual workers persist and acknowledge one or 37 pending events exactly once. |
| Replays double-count usage | Accept stream key/message ID pairs in a durable event ledger in the same transaction as history, counters, status, and refresh requests. Duplicate events within a batch, later replays, concurrent consumers, and transaction rollback are tested. |
| Rotators load entire pools | Creation and listing count distinct eligible routes in SQL. Credentials are loaded for selected or displayed routes. PostgreSQL creation and listing pass with 65,536 eligible routes. |
| Usage writes recount inventory | Check batches add usage deltas without querying managed inventory or overwriting capacity. Ownership and lifecycle writes maintain capacity. New check or traffic periods carry forward the last recorded active count using the usage-period index. |
| Client eviction scans the map | Use an LRU list, check hits before making room, and limit expiration cleanup per miss. Full-cache hits reuse their transport. Serial and concurrent distinct-route churn are benchmarked. |
| Uptime scans unrelated history | Correlate history to the candidate route before expanding workspace evidence. Workspace/configuration-specific validation and retained-history semantics remain intact. |
| Source counts recount every changed route's pool | Route notifications coalesce source/workspace pairs. A separate worker refreshes source counts, leaving proxy-list projection independent. Ten notifications for one route in a 10,000-route source produce one aggregate refresh. |
| Ownership completion erases new workspaces | Apply the worker's ownership delta to the current payload atomically, retaining workspaces added during its check. |
| Head repair strands additions | Read and update shard heads atomically in Lua for both proxy and scrape queues, without deleting another instance's indexed shards. |
| Long checks outlive or lose their lease | Renew before outbound attempts when fewer than two minutes remain. Completion and worker removal compare the owned lease score in Redis. Mark leased scores so atomic bulk scheduling and imports preserve active checks in every supported shard. Lost leases stop further attempts and failure tracking. |
| Reputation refresh has a fixed ceiling and volatile backlog | Persist one versioned refresh request per route in the statistics transaction. Configurable workers claim disjoint batches, drain continuously, recover expired claims, and preserve requests arriving during recalculation. Pending count and oldest age have Prometheus gauges. |

## Component measurements

A fixed 1,000-event usage write took approximately 4.7, 4.6, and 4.9 ms at
1,000, 100,000, and one million managed routes, respectively. The inventory
count that has been removed took approximately 0.17, 14.6, and 51.2 ms in
separate query-plan measurements. Timings cover usage accounting, not the
entire statistics transaction. The month rollover change adds indexed usage
period lookups on insert; it never reads the managed-route inventory.

Uptime selection for one route visited one retained history row with 200,000
unrelated rows present. The measured query took 18.6 ms. Creation and listing
for the 65,536-route rotator fixture together took 176 ms. Ten one-route source
notifications took 8.5 ms with no source aggregate scans; the separate source
worker then ran one full aggregate.

Cache churn medians use `GOMAXPROCS=4`, three one-second samples, and exclude
networking and transport construction. Concurrent churn uses 32 workers.

| Cache entries | Serial before | Serial after | Concurrent before | Concurrent after |
| ---: | ---: | ---: | ---: | ---: |
| 2,048 | 42.47 µs | 0.457 µs | 40.86 µs | 0.492 µs |
| 8,192 | 192.39 µs | 0.466 µs | 183.00 µs | 0.541 µs |
| 16,384 | 410.90 µs | 0.537 µs | 385.13 µs | 0.570 µs |

The LRU list increases the isolated replacement allocation from roughly
200 bytes to 400 bytes. The map remains capped at 16,384 entries.

## Checker operation budget

Current plaintext dequeue and normal requeue add no credential cryptography,
route fingerprinting, JSON encoding, payload writes, or database queries.
They still decode one queue payload and reuse its stored hash. Normal
completion sends only scheduling and lease data to Redis.

With loaded scripts, a normal queue cycle uses three client commands:
dequeue `EVALSHA`, interval `GET`, and completion `EVALSHA`. The previous cycle
used five commands across the same three network round trips. The completion
script adds one internal `ZSCORE` to fence stale workers before changing
scheduling. An expiring lease adds one `EVALSHA` containing `ZSCORE` and `ZADD`.
Worker orphan removal also compares the payload snapshot before deleting a
route, preserving imports published after the worker's ownership decision.
Renewal runs only on long routes, approximately once per three minutes of
continuous work. Supported individual requests are bounded by their 16-bit
millisecond timeout, at most 65.535 seconds.

The additional renewal command took a median 39.8 µs in three 20,000-command
Redis samples, with 456 allocated bytes and 14 allocations per renewal.

Real Redis cycle medians from three 20,000-cycle samples were 115.9 µs before
and 125.1 µs after with one worker. With 32 concurrent workers they were
42.7 µs before and 51.1 µs after. Allocations fell from 60 to 53 per cycle.
The measured scheduling cost is accepted because unfenced completion can
overwrite newer work, and unrenewed long checks can issue duplicate requests.
Repeat these measurements on deployment hardware before increasing workers.

## Deployment and limits

Run `--migrate-only` before starting the updated backend. It adds
`proxy_statistic_events` and `proxy_reputation_refreshes` without changing the
partitioned history table. Stop previous checker instances during the upgrade
so all active workers use lease renewal and fenced completion.

The event ledger retains one identity per accepted stream event after history
expires. Include its rows and primary key index in storage planning. The
in-memory fallback has no durable stream identity. Existing duplicated
history and counters are not repaired by this change because old events lack
a reliable stored event identity.

Reputation workers default to four per instance, with batches of 1,000 and a
one-second idle/error delay. Requests survive restarts and coalesce by route.
Monitor `magpie_proxy_reputation_refresh_pending` and
`magpie_proxy_reputation_refresh_oldest_age_seconds`; each instance reports the
same shared backlog. Source counts coalesce every 30 seconds for up to 100
source/workspace pairs by default. Their background refresh still aggregates
the source pool and can lag when pending pairs exceed that budget.

`PROXY_QUEUE_ENCRYPT_CREDENTIALS=false` remains the default. Legacy plaintext
and encrypted payload readers remain available, and `PROXY_ENCRYPTION_KEY`
must stay stable.

Validation includes the full normal and race suites, PostgreSQL and real
Redis recovery and scaling regressions, `go vet ./...`, `go build ./cmd/magpie`,
Compose configuration validation, and a Docusaurus production build. A
sustained tens-of-millions workload and multi-day soak remain release work.
