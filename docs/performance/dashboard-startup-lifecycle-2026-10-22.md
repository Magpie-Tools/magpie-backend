# Dashboard prewarm startup bound, 22 October 2026

The retained million-route run completed its offline derived-state repair, but
never opened the API within the unchanged 30-minute readiness deadline. Normal
bootstrap called `RefreshDashboardCaches` synchronously with the process context
before registering result consumers and most jobs. Dashboard caches accelerate
reads; their existing normal-startup error path was already optional.

The retained backend started at 17:17:06 UTC. Queue reconstruction finished at
17:27:36 after scheduling 999,999 routes and 10 scrape sites. At 17:47:08 SIGTERM
canceled the cache prewarm in workspace 8's judge-anonymity query. Setup logged a
warning, then started the consumers and API with an already-canceled process
context before shutting down. The readiness failure remains retained in
`validation-driver-20261022c`; its earlier full Go, race, vet, build, local image,
docs, distribution and startup checks passed.

Normal startup now gives the complete optional dashboard prewarm one five-second
context deadline. It waits synchronously for the refresh to finish or cancel;
there is no detached refresh goroutine. Completed cache components remain
available. A component that fails or cancels keeps its preceding snapshot, and
normal startup continues with the existing request-time fallbacks. Database
errors remain optional for this prewarm. Cancellation of the parent context
instead aborts the remaining setup and skips API initialization.

Development fixtures retain their existing strict prewarm behavior: a failed
prewarm aborts fixture startup. Their refresh uses the same five-second deadline.
Both paths use the database loaders' existing context propagation. The deadline
requests cancellation; startup waits for the loader to finish cancellation rather
than continuing while its work is still running.

Prewarming now precedes rotating listener startup. Read-model, statistics and
usage drains are registered before rotating listeners and checker/scraper
producers can accept work. Setup checks parent cancellation at startup
boundaries and between the remaining job registrations. The API checks its
context before initialization and again before listening. A normal signal during
startup uses the existing orderly shutdown path; an independent or joined setup
error remains an error. Producer, consumer and read-model shutdown order and the
configured shutdown deadlines are unchanged.

The regular dashboard refresh remains every 30 seconds. Readiness still requires
the database, configured Redis policy and startup queue bootstrap. The change
does not make a slow uncached dashboard acceptable: SQL scalability, real
dashboard response latency, coordinated restore and the fixed release gate still
require their own evidence. No load rate, latency threshold, maintenance setting,
worker profile or readiness timeout was widened.

## Focused checks

Go 1.27.3 with `GOMAXPROCS=4` ran this service-free selection against the final
lifecycle source, once normally and once with `-race`:

```text
go test ./internal/app ./internal/app/bootstrap ./internal/app/server \
  -run '^(TestDashboardPrewarm.*|TestSetupAlreadyCanceledDoesNotReadConfiguration|TestShutdown.*|TestServeAfterSetup.*|TestOpenRoutesHonors.*|TestHealthz.*|TestReadyz.*)$' \
  -count=1 -timeout 90s -v
```

Each mode passed 20 tests with zero skips. Normal took 5.780 seconds including
compilation; race took 9.544 seconds. Targeted vet passed. Tests cover the shared
prewarm deadline, parent cancellation, waiting for cancellation cleanup, a
refresh returning success after cancellation, strict fixture failures, releasing
the child context, canceled setup before configuration reads, preserving separate
startup errors, and refusing API initialization after cancellation. Existing
producer/consumer/read-model shutdown ordering and readiness policy tests were
included.

These focused checks use controlled callbacks and in-process fixtures. They do
not establish new PostgreSQL query performance, a new million-route startup pass,
or actual subprocess drain behavior. The existing PostgreSQL blocked-dashboard
query cancellation tests and app startup/shutdown subprocess tests are retained
for the combined normal/race run. Parent validation owns those full checks and
the actual recovery and load gate.

Raw commands, logs, hashes and the owner result are under
`.codex-tmp/release-remediation-20261020/dashboard-lifecycle-20261022/` in the
workspace. The independent database query follow-up has separate evidence under
`dashboard-startup-20261022/`.

## Checker operation budget

This changes startup scheduling and cancellation only. It adds no cryptographic
operations, route hashes, JSON encodes, Redis commands or database queries per
proxy check. The prewarm issues the existing queries with a bounded context; no
new startup query is added. Plaintext queue defaults, stored-hash reuse,
scheduling-only requeues, legacy readers and result persistence are unchanged.

Release artifact preparation and publication under item 4 remain deferred.
