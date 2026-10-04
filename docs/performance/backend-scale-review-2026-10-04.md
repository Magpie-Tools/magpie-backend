# Backend scaling review

Reviewed on 2026-10-04 at backend commit `9c3e77e3ac9dfdf8319e08eb2cfcbfc5618fcc43`. The target is tens of millions of proxy routes serving thousands of users through their workspaces. This review changes no runtime code or configuration.

The most urgent correctness problem is statistics recovery: the actual worker persisted 5,000 history rows from one pending Redis message. The largest measured scaling problems are repeated database aggregation and HTTP client cache eviction. Increasing worker counts alone will not address those costs.

P1 means fix before relying on this path at the requested scale. P2 means a capacity constraint that needs an explicit design or sizing decision. Measurements below are local component measurements, not production capacity claims.

1. **[P1] Pending statistics recovery repeats messages until the buffer fills.**

   Sources: [pending cursor initialization](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:471), [read loop](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:524), [missing cursor advancement](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:548).

   Recovery reads the consumer's pending entries using `streamID = "0"`. After a nonempty read, it appends the messages but neither advances that cursor nor acknowledges the page. The next read returns the same pending entries. With fewer than 5,000 pending entries, the buffer fills with repeated copies before persistence runs. Startup claims can populate the consumer's pending list even when it has a new consumer name.

   Reproduction used the actual recovery worker, real Redis 7, and PostgreSQL 17. One pending message produced 5,000 `proxy_statistics` rows at the default batch threshold. This can corrupt history and check counters while multiplying persistence work during recovery.

   Advance the pending cursor to the last received message ID, or persist and acknowledge each pending page before rereading it. Add recovery tests with one pending message and a partial batch. The separate idempotency issue below still needs fixing.

2. **[P1] Statistics replay double-counts committed events.**

   Sources: [commit before stream acknowledgement](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:627), [unconditional insert and counter increments](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_proxy_statistics_handl.go:89).

   PostgreSQL commits before Redis acknowledges the messages. A crash in between, or a stale-message claim, can replay an already committed event. The stream message ID is not passed into persistence as an idempotency key. Generated statistic IDs identify database rows, rather than the original event, and latest-status upserts do not deduplicate history or counters.

   Persisting the same serialized event twice in PostgreSQL produced two history rows, two daily checks, and two workspace check attempts. Use the existing Redis stream message ID as a durable event identity and increment counters only for newly accepted events in the same transaction. This avoids introducing a cryptographic identity operation into the checker.

3. **[P1] Rotator creation and listing load every eligible proxy and hit PostgreSQL's parameter limit.**

   Sources: [creation](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_rotating_proxy_handler.go:167), [listing](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_rotating_proxy_handler.go:251), [unbounded candidate loading](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_rotating_proxy_handler.go:402), [unbatched credential lookup](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/proxy_access_credentials.go:29).

   Both callers load full candidate rows and hydrate their credentials to calculate `AliveProxyCount`. The credential query puts every candidate ID into one `IN` clause. A fixture with 65,536 eligible proxies failed with `extended protocol limited to 65535 parameters`. The REST list handler turns this into HTTP 500, and creation rolls back. Below that limit, the path still retains and decrypts the entire eligible pool for a count.

   Count eligible proxies in SQL. Load credentials only for the selected route or the last route that must be displayed. Batching the existing lookup would remove the parameter error but preserve the large memory and decryption cost.

4. **[P1] Every statistics batch recounts the participating workspaces' entire active inventory.**

   Source: [workspace usage aggregation](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_workspace_usage.go:72).

   `recordWorkspaceCheckUsage` runs `COUNT(*)` over all active managed proxies in every participating workspace, inside the statistics transaction. Its work grows with the combined workspace inventory, rather than the number of events being persisted. A batch spread across many workspaces can repeatedly scan a large part of the installation. An index does not remove the need to visit all matching entries.

   A fixed batch of 1,000 events for one workspace measured the following. Each value is the median of five usage-write transactions; the query-plan measurement is a separate execution.

   | Active managed proxies | Usage-write median | Count-query execution | Matching rows visited |
   | ---: | ---: | ---: | ---: |
   | 1,000 | 4.0 ms | 0.13 ms | 1,000 |
   | 100,000 | 17.0 ms | 9.88 ms | 100,000 |
   | 1,000,000 | 45.0 ms | 49.43 ms | approximately 1,000,000 |

   Keep active-route accounting on ownership and lifecycle changes, with occasional reconciliation. Statistics persistence should add check-attempt deltas without recounting or overwriting active capacity on each batch.

5. **[P1] HTTP client eviction scans the entire cache under one shared mutex.**

   Sources: [maintenance before lookup](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/http_client_cache.go:72), [eviction](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/http_client_cache.go:131), [linear oldest-entry search](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/http_client_cache.go:176).

   The cache key includes the proxy route, judge, protocol, transport, and timeout. A scan through millions of different routes keeps it full and causes sustained eviction. Every eviction iterates over the whole map while all checker workers share the mutex. More workers enlarge the cache and make this serialized work slower. Maintenance also evicts an entry before checking for a hit; a reproduction showed an oldest-entry hit rebuilding its transport.

   An isolated cache-churn benchmark measured maintenance and replacement, excluding network requests and transport construction. It used `GOMAXPROCS=4`, three one-second samples, on an Intel Core i7-12700H.

   | Worker count used to size cache | Cache entries | Median time per replacement |
   | ---: | ---: | ---: |
   | 500 | 2,048 | 42.5 microseconds |
   | 2,000 | 8,192 | 191.9 microseconds |
   | 5,000 | 16,384 | 413.2 microseconds |

   Check for hits before making room, and use bounded eviction with constant or amortized constant cost. Sharding can reduce contention. Validate with distinct-route churn and concurrent workers; repeated requests through one proxy do not exercise this bottleneck.

6. **[P1] Rotator uptime filtering scans unrelated retained history during selection.**

   Sources: [uptime aggregation](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_rotating_proxy_handler.go:499), [candidate selection](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_rotating_proxy_handler.go:456), [rotator row lock](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/database_rotating_proxy_handler.go:317).

   The uptime subquery aggregates `proxy_statistics` by route, workspace, and configuration key. It filters protocol and transport but does not first restrict history to the candidate routes or a time window. The final `LIMIT 1` does not guarantee bounded aggregation. Selection runs while holding the rotator row lock, so this cost also delays concurrent requests to the same rotator.

   PostgreSQL `EXPLAIN ANALYZE` for a target workspace with one route visited 200,001 history rows after adding 200,000 rows belonging to another workspace. Execution took 283.69 ms. Maintain uptime aggregates by workspace, route, protocol, and configuration key, or scope history to a small candidate set before calculating uptime. Preserve workspace-specific validation.

7. **[P1] One changed proxy triggers a full recount of its scrape sources.**

   Sources: [source refresh after route projection](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/read_model_indexes.go:256), [affected-source lookup](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/read_model_indexes.go:544), [full source aggregation](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/database/read_model_indexes.go:595).

   Refreshing a batch of changed routes finds their sources, then recounts every proxy attached to those sources for their subscribing workspaces. This includes a current-health lookup for each association. A large, widely subscribed source can therefore be rescanned repeatedly as normal checks complete, even when only one route changed. Source aggregation shares the refresh loop with the proxy-list projection, so it can delay list health updates too.

   A one-route update against a source containing 10,000 routes visited all 10,000 source links. The refresh took 92.68 ms; a separate query-plan execution took 65.41 ms. Coalesce dirty source/workspace pairs separately from route projection, or maintain source health counts from state changes. Keep full reconciliation off the frequent update path.

8. **[P1] A stale ownership-changing requeue can erase a newly added workspace.**

   Sources: [unconditional payload replacement](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:685), [failure-driven owner removal](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/thread_handler.go:208), [refresh uses only queued IDs](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/thread_handler.go:275).

   A worker dequeues a route owned by workspace A. Workspace B imports that route while the check runs and publishes a payload containing A and B. If the worker then removes A following a failure action, `RequeueProxyWithPayload` replaces the newer payload with the worker's old owner list after removing A. B disappears. The ownership verifier can still see B in PostgreSQL and keep scheduling the route, but later workers cannot discover B because refresh starts from queued IDs.

   A deterministic queue reproduction left the next dequeued payload with no workspace IDs after B had been added. Apply ownership changes against the current payload using a version check or atomic mutation, with authoritative reconciliation on conflicts. Keep this work on the ownership-change path; ordinary requeues should continue to update scheduling only.

9. **[P1] Queue-head rebuilding can strand concurrently enqueued work.**

   Source: [head rebuilding](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:196).

   `refreshQueueHeads` reads shard heads, then executes a pipeline that deletes and rebuilds the shared head index from those earlier reads. An enqueue into an empty shard between the read and pipeline execution creates a head that the rebuild deletes. Dequeue reads only the head index and cannot discover the stranded member. Constructors run this rebuilding when instances start, which makes the race relevant to rolling restarts and scaling out.

   A deterministic reproduction left one route in a shard with zero indexed heads; dequeue timed out. Another enqueue or rebuild can repair the shard, but there is no repair in the normal empty-head dequeue branch. Rebuild heads atomically from current shard state, or use updates that cannot erase concurrent additions. The scrape queue has the same rebuilding pattern and should receive the corresponding fix.

10. **[P1] Supported check budgets can outlast the fixed five-minute processing lease.**

    Sources: [lease duration](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/proxy_queue.go:38), [lease assignment](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/queue/proxy/pop.lua:46), [sequential assignments](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/thread_handler.go:439), [retry loop](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/checker/thread_handler.go:568).

    A valid 60-second timeout with five retries permits six minutes of work for one assignment. Shared routes with different workspace profiles can run several assignments sequentially and exceed the lease with much smaller individual budgets. There is no lease renewal or completion token. Another worker can dequeue the same route while its original worker is still checking it, duplicating requests and failure tracking. A stale completion can then overwrite scheduling established by a newer worker.

    A queue-script reproduction dequeued the same route again after advancing the caller-supplied time past the lease, without any completion or requeue. Renew long-running leases and fence completion by lease ownership, or explicitly bound total route work below the lease. Any additional queue commands need representative throughput measurements under the repository's hot-path rules.

11. **[P2] Reputation refresh has a fixed throughput ceiling and no backlog bound.**

    Sources: [fixed budgets](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:30), [dirty set](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:680), [four-batch limit](/home/kuchen/IdeaProjects/MagpieWorkspace/magpie-backend/internal/jobs/runtime/proxy_statistics.go:799).

    Each instance recalculates at most four batches of 5,000 route reputations, then waits another minute after processing finishes. The optimistic ceiling is 20,000 distinct routes per minute, or 333 per second, and database time lowers it. Increasing statistics ingest workers does not increase this coordinator's budget. The dirty set can grow to millions of route IDs, results become stale, and outstanding work is lost on restart.

    At 30 million unique routes checked every five hours, updates arrive at approximately 1,667 routes per second. At least five such coordinator budgets would be needed even with zero processing time and perfect distribution. Make the work budget and concurrency configurable, bound or persist pending work, and measure backlog age. This is a code-derived capacity limit, not a measured end-to-end rate.

For capacity planning, the check interval and number of distinct physical requests matter as much as route count. The following arithmetic assumes 30 million globally distinct routes, a five-hour interval, no extra profile fanout, and 30 days of retained history. Retries add outbound attempts but currently produce one persisted event per completed assignment.

| Workload | Route cycles/second | Statistics/second | History rows/day | History rows over 30 days |
| --- | ---: | ---: | ---: | ---: |
| One physical request per route cycle | 1,667 | 1,667 | 144 million | 4.32 billion |
| Four physical requests per route cycle | 1,667 | 6,667 | 576 million | 17.28 billion |

These are workload counts, not measured storage sizes. Sharing a request saves network work, but workspace evidence and latest-status rows still grow with participating workspaces. Different judges and budgets can increase physical request fanout. With the default 7.5-second timeout and two retries, requests that exhaust all attempts require approximately 37,500 concurrent worker slots to sustain the first workload, before other overhead.

The current plaintext queue path preserves the intended stored-hash reuse and scheduling-only normal requeue. Leave `PROXY_QUEUE_ENCRYPT_CREDENTIALS=false` as the default and keep the existing compatibility readers. None of these findings calls for adding credential cryptography or database credential loading to the ordinary checker loop.

Validation completed:

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `go build -o /tmp/magpie-backend-review-20261004/magpie ./cmd/magpie`
- Temporary queue race and lease reproductions, cache hit reproduction, and cache-churn benchmarks through a Go overlay.
- PostgreSQL 17 probes for duplicate accounting, the rotator parameter limit, inventory recount scaling up to one million routes, uptime query plans, and source query plans.
- Actual statistics recovery worker against disposable Redis 7 and PostgreSQL 17.

The full normal and race suites used the repository defaults; environment-gated PostgreSQL tests were not enabled for those runs. The targeted PostgreSQL probes ran separately. The review probes assert the faulty behavior, so their passing results confirm the reproductions. No sustained tens-of-millions workload or thousand-workspace soak was run.

Local reproduction sources remain available through [the Go overlay](/tmp/magpie-backend-review-20261004/overlay.json), with [checker probes](/tmp/magpie-backend-review-20261004/checker_review_test.go), [queue probes](/tmp/magpie-backend-review-20261004/queue_review_test.go), [database probes](/tmp/magpie-backend-review-20261004/database_review_test.go), and [stream recovery probe](/tmp/magpie-backend-review-20261004/runtime_review_test.go). They were not added to the production packages. The disposable databases were removed after validation.

Fix the recovery cursor and event idempotency first, then the rotator candidate count. Remove the repeated large aggregations and linear cache eviction before increasing worker counts. Queue ownership and lease fixes should retain the existing operation-budget regressions and include measurements for any additional commands.
