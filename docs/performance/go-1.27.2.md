# Go 1.27.2 security update

The [backend CI run](https://github.com/Magpie-Tools/magpie-backend/actions/runs/37998684820) passed tests, race tests, vet, and build, then failed its vulnerability scan. `govulncheck` 1.8.0 reported eleven reachable advisories in Go 1.27.1's standard library. [Go 1.27.2](https://go.dev/doc/devel/release#go1.27.2), released on 2026-10-08, supplies the fixes. The associated HTTP/2 fixes also require [`golang.org/x/net` 0.60.0](https://pkg.go.dev/vuln/GO-2026-6610).

The module now requires Go 1.27.2, CI selects that version from `go.mod`, and the Docker builder pins `golang:1.27.2-alpine`. `x/net` advances from 0.59.0 to 0.60.0. Other module versions stay at their existing pins. This update requires no application migration or configuration change.

The application checker and queue source is unchanged. The normal plaintext queue path adds zero cryptographic operations, JSON encodes, Redis commands, or database queries per proxy check. Queue encryption remains opt-in, stored hashes are reused, and normal requeues preserve the payload.

## Validation

Validated on 2026-10-10 with disposable PostgreSQL 17 and Redis 7 instances. The full normal and race suites enable PostgreSQL integration tests with `MAGPIE_TEST_POSTGRES_DSN`. Checks cover the existing plaintext stored-hash and payload-preservation regressions.

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `go build ./cmd/magpie`
- `go mod verify`
- `go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 -show verbose ./...`
- Static Linux builds with `CGO_ENABLED=0` for both amd64 and arm64
- Docker builder manifest resolution for `golang:1.27.2-alpine`
- Documentation build and typecheck

The vulnerability scan reports zero reachable vulnerabilities and zero advisories in imported packages. It still lists the existing module-only advisory for the unimported, unmaintained `x/crypto/openpgp` package. No scanner suppression or CI check was removed.

## Paired queue and checker measurements

Both variants use the same application source. The baseline binary uses Go 1.27.1 and `x/net` 0.59.0; the updated binary uses Go 1.27.2 and `x/net` 0.60.0. Five one-second samples per variant alternate execution order on the same host, with `GOMAXPROCS=4`. Client processes use four CPUs and the disposable Redis instance uses a separate CPU. CPU placement does not reserve cores from other host processes.

The table shows medians in microseconds per operation. The concurrent queue benchmark shares one client among 32 workers; its time per operation is elapsed time divided by total completed operations, rather than individual request latency.

| Benchmark | Before, µs/op | After, µs/op | Time change | Allocations before / after |
| --- | ---: | ---: | ---: | ---: |
| Plaintext decode, one workspace | 1.371 | 1.283 | -6.4% | 5 / 5 |
| Plaintext decode, eight workspaces | 2.474 | 2.336 | -5.6% | 8 / 8 |
| Redis dequeue/requeue, one worker | 116.734 | 111.715 | -4.3% | 53 / 53 |
| Redis dequeue/requeue, 32 workers | 83.136 | 82.970 | -0.2% | 53 / 53 |
| HTTP check, 2KiB response | 19.333 | 19.379 | +0.2% | 92 / 93 |
| HTTP check, 64KiB response rejected at 8KiB | 41.819 | 44.506 | +6.4% | 104 / 105 |

Queue allocation counts are unchanged. HTTP checks use one additional allocation per operation. Oversized-response rejection has a higher median, with broadly overlapping samples: 24.528 to 65.977 µs before and 24.867 to 69.327 µs after. Small-response samples also overlap, at 11.473 to 29.142 µs before and 11.632 to 24.950 µs after. These component measurements do not establish sustained deployment throughput or prove a repeatable timing regression.

Reproduce using the existing `BenchmarkCurrentPlaintextProxyDecode`, `BenchmarkProxyQueueCycle`, `BenchmarkProxyQueueCycleConcurrent`, and `BenchmarkProxyCheckRequest` benchmarks. Run queue cycles only against an empty disposable Redis database via `MAGPIE_BENCH_REDIS_URL`, and compare separately compiled baseline and updated binaries with identical benchmark flags:

```text
-test.run='^$' -test.bench='^Benchmark' -test.benchmem -test.benchtime=1s -test.count=1 -test.cpu=4
```

Restrict the queue benchmark pattern to the three names above; legacy migration and lease-renewal benchmarks measure different workloads. Alternate variant order and collect five samples for each benchmark.
