# Go 1.27.1 and dependency upgrade

Validated on 2026-10-03. The backend builds and passes its tests on Go 1.27.1
with the dependency versions below. The Docker builder pins the same patch;
CI reads the Go version from `go.mod`.

## Dependency changes

| Module | Before | After |
| --- | --- | --- |
| `github.com/PuerkitoBio/goquery` | 1.12.0 | 1.13.0 |
| `github.com/alicebob/miniredis/v2` | 2.37.0 | 2.39.0 |
| `github.com/jackc/pgx/v5` | 5.9.2 | 5.11.0 |
| `github.com/prometheus/client_golang` | 1.23.2 | 1.24.1 |
| `github.com/quic-go/quic-go` | 0.59.0 | 0.63.0 |
| `github.com/redis/go-redis/v9` | 9.18.0 | 9.22.0 |
| `golang.org/x/crypto` | 0.50.0 | 0.57.0 |
| `golang.org/x/net` | 0.53.0 | 0.59.0 |
| `golang.org/x/sync` | 0.20.0 | 0.23.0 |
| `gorm.io/driver/postgres` | 1.6.0 | 1.6.3 |
| `gorm.io/gorm` | 1.31.1 | 1.31.2 |

Transitive updates include `golang.org/x/text` 0.36.0 to 0.42.0,
`golang.org/x/sys` 0.43.0 to 0.48.0, `github.com/mattn/go-sqlite3` 1.14.42
to 1.14.52, and `github.com/klauspost/compress` 1.18.5 to 1.20.1. See
`go.mod` and `go.sum` for the complete dependency set.

The initial vulnerability scan found eight reachable advisories in `x/net`,
`x/text`, and `quic-go`. With the updated versions, `govulncheck` 1.8.0 reports
zero reachable vulnerabilities. Its verbose output still lists the
module-only advisory for the unused, unmaintained `x/crypto/openpgp` package.
Magpie does not import that package.

## Compatibility checks

These checks passed with Go 1.27.1:

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `go build ./cmd/magpie`
- `go mod verify`
- `go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...`
- `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./cmd/magpie`
- `docker build --target backend-build .`
- `go test -tags integration ./internal/support -run '^TestRedisSentinelFailover_RecoversWritesAfterPrimaryDown$' -count=1 -v`

Normal and race tests included PostgreSQL integration checks through
`MAGPIE_TEST_POSTGRES_DSN`, using a disposable PostgreSQL 17 database. Enabling
those checks exposed an existing read-model fixture that lacked workspace join
table configuration and used a user ID as a workspace ID. The fixture now
matches the workspace schema; production migration code is unchanged.

The new HTTP/3 test exercises both HTTP3 and QUIC transports over a real local
UDP connection. It checks the judge authority, path, protocol, and response
with a trusted test certificate. Existing queue tests still prove hash reuse
without an encryption key and payload preservation on normal requeue.

[Go 1.27](https://go.dev/doc/go1.27) changes JSON internals and allows HTTP
response-body close to drain unread bytes within limits. A streaming-response
regression test verifies that rejecting an oversized judge response returns
within 500ms even when the server stalls. The local HTTP benchmark also covers
complete oversized responses.

The [Redis client releases](https://github.com/redis/go-redis/releases) change
default read/write timeouts from 3s to 5s and retry backoff from 8ms/512ms to
10ms/1s. TCP keep-alive now starts probing after 30 seconds of idle time,
previously five minutes. Magpie retains those upstream defaults.
Single-instance deployments can override the timeout and retry settings in
`REDIS_URL`. The isolated Sentinel test recovered writes after pausing its
primary and completed in 18 seconds. Include stalled Redis latency in
deployment validation.

## Queue operation budget

The application queue/checker implementation is unchanged. For a ready,
current plaintext payload with a valid stored hash, the normal dequeue and
schedule-only requeue has this budget:

| Operation | Before | After | Added per cycle |
| --- | ---: | ---: | ---: |
| Credential encryption/decryption, nonce generation | 0 | 0 | 0 |
| Route fingerprint calculation | 0 | 0 | 0 |
| JSON encodes | 0 | 0 | 0 |
| JSON decodes | 1 | 1 | 0 |
| Queue-issued Redis wire commands | 5 | 5 | 0 |
| Database queries in queue operations | 0 | 0 | 0 |
| Payload writes | 0 | 0 | 0 |

The five wire commands are one cached `EVALSHA`, one interval `GET`, and a
pipeline containing one `ZREM` and two `ZADD` commands. The Lua dequeue makes
six Redis data calls on the normal valid-head path. These use three client
round trips in total. Initialization, reconnects, stale heads, and legacy
payload migration are outside this steady-state count.

Queue credential encryption stays opt-in, and legacy plaintext and encrypted
payload readers remain available. No deployment encryption key changes are
required.

## Measurement method

The benchmark host ran Linux amd64 on an Intel Core i7-12700H. Each binary used
`GOMAXPROCS=4`, five 1-second benchmark samples, and alternating version order.
Redis 7 ran in an isolated local Docker container without persistence; the
queue benchmark used an empty dedicated database. HTTP checks used a local
HTTP proxy and an 8KiB response limit. Binaries ran sequentially with the same
configuration.

The original module specified Go 1.26.2. Its comparison binary used the
installed `go1.26.8-X:nodwarf5` compiler. A second comparison used that same
compiler with the updated dependencies to separate dependency changes from
Go 1.27.1. This host's Go 1.26 build differs from the standard upstream build
in its DWARF setting, so repeat measurements with the production toolchains
before relying on exact timing percentages.

Run the benchmarks from the backend repository against disposable Redis:

```bash
REDIS_URL=redis://127.0.0.1:16379 \
MAGPIE_BENCH_REDIS_URL=redis://127.0.0.1:16379/15 \
GOMAXPROCS=4 go test -p 1 ./internal/jobs/queue/proxy ./internal/jobs/checker \
  -run '^$' -bench '^Benchmark' -benchmem -benchtime=1s -count=5
```

The queue cycle seeds a ready route for every iteration before timing starts.
Only the real dequeue, payload decode, and normal requeue run inside the timed
loop. No score-reset commands or sleep are included.

## Results

The tables show medians from five samples. Times are microseconds per
operation; allocation columns show allocations per operation.

### Original dependencies versus the upgrade

| Benchmark | Go 1.26.8, old dependencies | Go 1.27.1, updated dependencies | Time change | Allocations before / after |
| --- | ---: | ---: | ---: | ---: |
| Plaintext decode, one workspace | 2.550 | 1.481 | -41.9% | 13 / 5 |
| Plaintext decode, eight workspaces | 3.945 | 2.699 | -31.6% | 16 / 8 |
| Redis dequeue/requeue | 152.297 | 159.179 | +4.5% | 69 / 60 |
| HTTP check, 2KiB response | 14.445 | 15.081 | +4.4% | 91 / 91 |
| HTTP check, 64KiB response rejected at 8KiB | 47.528 | 27.190 | -42.8% | 158 / 103 |

### Compiler comparison with identical updated dependencies

| Benchmark | Go 1.26.8 | Go 1.27.1 | Time change | Allocations before / after |
| --- | ---: | ---: | ---: | ---: |
| Plaintext decode, one workspace | 2.697 | 1.441 | -46.6% | 13 / 5 |
| Plaintext decode, eight workspaces | 4.211 | 2.573 | -38.9% | 16 / 8 |
| Redis dequeue/requeue | 138.166 | 135.990 | -1.6% | 68 / 60 |
| HTTP check, 2KiB response | 14.350 | 15.474 | +7.8% | 91 / 91 |
| HTTP check, 64KiB response rejected at 8KiB | 46.817 | 26.026 | -44.4% | 158 / 103 |

The allocation reductions repeat across every sample. One-workspace decode
uses 792 bytes instead of 1,096. In the compiler comparison, median bytes per
operation drop from 4,239 to 3,939 for the queue cycle and from 103,671 to
91,933 for oversized HTTP checks.

Queue timing samples overlap substantially. These two comparisons use
different dependency baselines, so the Go-only comparison cannot exclude a
Redis client regression. The focused follow-up below isolates that change.

The small-response HTTP median is 4.4% to 7.8% slower on Go 1.27.1. Its sample
ranges also overlap, and allocations stay at 91 per request. Treat that as a
latency concern to recheck under sustained deployment load. The JSON and
oversized-response timings improve in both comparisons.

### Focused queue follow-up

The initial +4.5% queue median represents 6.882 microseconds per cycle. If it
were repeatable and the queue were the throughput bottleneck, that would mean
roughly 4.3% less queue capacity. It warranted a separate check.

This follow-up used six 3-second samples for each variant, rotating and
reversing variant order. Clients ran on CPUs 0, 2, 4, and 6, four physical
performance cores; Redis ran on CPU 8, a separate performance core. The host
was still shared, so CPU placement did not reserve those cores or eliminate
timing variation.

Both sequential and concurrent benchmarks execute the actual queue cycle.
The concurrent case shares one client and queue between 32 workers. To
isolate the Redis client, a third binary used Go 1.27.1 and the updated module
set with only `go-redis` restored to 9.18.0 and its required transitive module
added. The application queue code was identical in all three variants.

| Queue workload | Original Go/dependencies, µs/op | Upgrade, µs/op | Upgrade time change | Old Redis on Go 1.27.1, µs/op | Redis-only time change |
| --- | ---: | ---: | ---: | ---: | ---: |
| One worker | 127.833 | 123.567 | -3.3% | 127.142 | -2.8% |
| 32 concurrent workers | 75.198 | 74.075 | -1.5% | 74.326 | -0.3% |

Concurrent time per operation is total benchmark wall time divided by the
number of completed cycles, the reciprocal of aggregate throughput. It is
not the latency experienced by an individual worker. The sequential and
concurrent rows measure different workloads and should not be compared as
request latencies.

Allocations remain 69 per cycle with the original versions, 60 with the
upgrade, and 61 with the old Redis client on Go 1.27.1. This repeats in both
workloads and every sample.

The +4.5% median increase did not reproduce, and the isolated Redis comparison
shows nearly identical concurrent throughput. The original samples ranged
from 127.9 to 167.3µs for the baseline and 106.9 to 173.0µs for the upgrade.
The follow-up also has substantial variation, so these measurements support
an inconclusive queue timing result rather than proving either a regression
or a small speedup. The allocation reduction is consistent. Recheck sustained
checker throughput, queue wait percentiles, and Redis CPU on deployment
hardware before making a production capacity claim.

These are local component measurements. They do not establish capacity for a
million-proxy deployment or replace the sustained load and soak checks in the
[distribution performance validation scripts](https://github.com/Magpie-Tools/magpie/blob/main/scripts/perf/README.md).
