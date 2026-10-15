# Production readiness fixes, 14 October 2026

The four release blockers from the assessment of backend commit
`701500f9e12f77e8e1a8c0adcd54faca1e64b55b` have fixes and regression coverage
in the working tree. Deployment and documentation changes belong to the sibling
`magpie` and `magpie-docs` repositories. These results establish the tested
behaviors below; target capacity still needs sustained workload evidence.

## Resulting behavior

- The image provisions settings and browser-cache directories for UID/GID
  65532. Compose repairs ownership of existing volumes before starting the
  nonroot backend. The release startup script checks fresh volumes, root-owner
  repair, generated-key persistence, production registration rejection and
  graceful container exit.
- Browser contexts use a restricted local proxy without direct fallback or
  loopback bypass. Resolution and validation happen before dialing the chosen
  literal address, for both HTTP forwarding and CONNECT. Chromium's divergent
  resolver cannot select a private destination after Go's validation.
- Both installers generate a 64-character random hexadecimal JWT signing
  secret and persist it in `.env`. An empty `JWT_SECRET` makes the backend
  generate and atomically publish a mode-0600 key at `data/jwt_secret`. Concurrent
  startup selects one complete key; restart reuses it. Custom secrets remain in
  use. The old published Compose default selects the persistent generated key
  instead, invalidating tokens signed with that default. The proxy encryption
  key stays unchanged.
- Bootstrap returns a lifecycle handle. API drain precedes producer shutdown;
  dispatchers join their workers, scraper post-processing drains, rotating
  listeners join tracked clients, and statistics/usage consumers finish before
  Redis closes. The heartbeat cleanup also completes before client closure.
  Background shutdown has a 60-second limit after the API's 20-second drain.
  Compose allows 90 seconds before forced termination.
- Base Compose keeps local first-account setup available through the explicit
  local setup switch. Production mode ignores that switch. The production
  overlay enables strict validation, disables registration/bootstrap by
  default and protects observability. Compose now forwards the hardening
  controls and binds the API to loopback by default.
- Snapshot measurement includes legacy queues plus every numeric queue shard,
  and statistics storage includes the parent, descendant partitions and their
  indexes. The release gate requires at least 24 hours of mixed soak and usable
  drop-budget logs. Short checks have an explicit smoke mode.

## Validation

Validation used Go 1.27.2 on Linux amd64, disposable PostgreSQL 17 and Redis 7,
and a newly built backend runtime image. Service-backed suites used isolated
databases and the race suite used a race-built application subprocess.

| Check | Result |
| --- | --- |
| `go test ./... -count=1` with disposable services | Pass |
| `go test -race ./... -count=1` with disposable services | Pass |
| `go vet ./...` | Pass |
| `go build ./cmd/magpie` | Pass |
| `govulncheck@v1.8.0 ./...` | No reachable vulnerabilities; one advisory in an unused module package |
| Runtime image build | Pass |
| Fresh/repaired-volume production startup and graceful exit | Pass |
| Chromium DNS-divergence regression | Pass; private fixture received zero requests |
| Chromium rendering, cookie isolation, HTTPS, WebSocket, cleanup and startup lifetime tests | Pass |
| Real application SIGTERM with two-second delayed database inserts | Pass; one request and 123 bytes persisted with healthy Redis, plus one statistic through the memory path |
| Installer/update and release-gate regression suite | Pass; five tests |
| Base/production Compose configuration and Bash syntax | Pass |
| Docusaurus documentation build | Pass |
| Read/write/mixed performance smoke | Pass; ten seconds per suite, zero HTTP error rate and zero observed statistic drops |

The snapshot fixture contained members in both legacy queues and numeric
shards, including proxy shard 90 and scrape shard 40. Snapshot totals were four
proxy members and three scrape members. PostgreSQL's statistics parent reported
zero bytes while the corrected snapshot reported 196,608 bytes for its storage
tree. Redis snapshot collection succeeded with authentication enabled.

The smoke run started with one route and ended with 5,501 routes. The checker and scraper ran with minimal worker settings, no judges and no
scrape sources; backend `GOMAXPROCS` was two. It verifies that the suites, budgets and artifact collection execute. It
does not measure live checker throughput or establish production capacity.

Local verification logs are `/tmp/magpie-fixes-test-all-final.log`,
`/tmp/magpie-fixes-race-all-final.log`, `/tmp/magpie-fixes-shutdown.log`,
`/tmp/magpie-fixes-browser-final.log`, `/tmp/magpie-fixes-startup-final.log` and
`/tmp/magpie-fixes-perf-smoke.log`. The smoke snapshots and JSON summaries are
also copied into `magpie/artifacts/perf/2026-10-14-readiness-smoke/`, which is
ignored by Git.

## Checker operation budget

No dequeue, payload decode, check request or normal requeue logic changed.
Worker join bookkeeping runs at worker startup/exit. The following additions
per proxy check are all zero:

| Operation | Added per check |
| --- | --- |
| Encryption, decryption, nonce generation or route-hash calculation | 0 |
| JSON encodes | 0 |
| Redis commands | 0 |
| Database queries | 0 |

Plaintext Redis queue credentials remain the default. Stored-hash reuse,
scheduling-only requeues and legacy payload readers remain covered by the
existing normal and race suites. `PROXY_ENCRYPTION_KEY` remains stable.

## Remaining release evidence

A representative 24-to-72-hour soak, backup restore exercise and months of
longevity evidence remain unperformed. The optional nginx export and
200,000-rule alert cadence checks were not run in this task. Record hardware,
route volume, tenant sharing, real checker/scraper concurrency and maintenance
settings with the sustained test results before making million-scale claims.
Keep logs for the complete gate window, including rotations.
