# Share judge picks and stop rewriting dead-route streaks

A review of scaling to thousands of workspaces found three costs that grow with
the number of owners of a route. This change fixes two of them. The third, which
moves evidence and streaks from workspaces to configuration keys, is a schema
change proposed in
[ADR 0013](../adr/0013-scope-shared-route-health-to-checker-configuration.md).

## Shared judge picks

Requests merge only when the judge matches, but each workspace snapshot kept its
own round-robin counter. Owners with identical judge lists therefore usually
picked different judges in the same cycle. With five judges, a shared route sent
up to five requests per protocol and stored five statistics where one would do.

`checkerconfig.Build` now records an interned identity for each protocol's
ordered judge IDs. In `buildRequestAssignments`, the first owner with a given
identity advances its counter and the other owners reuse that position. Each
owner still validates with its own regex and headers. Single-owner routes skip
the coordination entirely, so their rotation is unchanged.

`BenchmarkBuildRequestAssignmentsSharedRoute` models 1,000 owners on default
settings with five judges, HTTP and SOCKS5, and desynchronized counters. It ran
3,000 iterations per sample, alternating old and new binaries six times.

| Variant | Physical requests per check | Median time per check | Bytes allocated | Allocations |
| --- | ---: | ---: | ---: | ---: |
| Per-workspace counters | 10 | 787 µs | 526,379 | 4,117 |
| Shared pick | 2 | 700 µs | 422,371 | 4,038 |

The time difference is within run-to-run noise (old 606–1,128 µs, new
560–866 µs). Outbound requests, statistics rows and their evidence fall
fivefold for this route.

## Saturated failure streaks

The failure increment used `LEAST(streak + 1, 65535)`, which created a new row
version of the wide `user_proxies` row for every owner on every failed check,
even after the streak stopped mattering. The increment now skips rows at 255.
`AutoRemoveFailureThreshold` is a `uint8`, so 255 meets every threshold, and the
reputation penalty saturates at 5.

Skipped rows are absent from `RETURNING`. Previously, an absent row made
`ensureFailureCounts` issue a follow-up `SELECT`. The tracker now treats an
absent row as saturated, which removes that query. The only other absent row is
an ownership removed during the check, for which pause and delete affect zero
rows.

A first version reported saturated rows through a `UNION ALL` read in the same
statement. It made a failing route below saturation about 1.5 ms slower per
200-owner chunk and was discarded.

`TestSaturatedFailureStreaksSkipWritesPostgres` ran on PostgreSQL 17 in a local
container with default settings. It used 1,000 workspaces × 50 routes, with
240-byte credential columns, and one 200-owner chunk. Each statement ran 11
times, interleaved with the other and rolled back after each run.

| Chunk | Statement | Median SQL time | WAL records | WAL bytes |
| --- | --- | ---: | ---: | ---: |
| Dead route, 200 saturated owners | Previous | 2.1–3.8 ms | ~994 | ~121,800 |
| Dead route, 200 saturated owners | Saturating | 0.4–0.8 ms | 0 | 0 |
| Failing route below saturation | Previous | 2.5–4.3 ms | ~1,014 | ~124,700 |
| Failing route below saturation | Saturating | 2.5–4.4 ms | ~1,014 | ~124,700 |

Ranges span repeated test runs on a shared development machine. A route
reaches saturation after 255 failed cycles, roughly 43 hours at a 10-minute
interval, and then stops generating streak writes until it succeeds.

## Hot-path operation counts

| Category | Added per proxy check |
| --- | --- |
| Cryptographic operations, nonces, route fingerprints | 0 |
| JSON encodes | 0 |
| Redis commands | 0 |
| Database queries | 0 (one conditional `SELECT` removed) |

Judge sharing adds one small map lookup per owner and protocol for routes with
more than one owner. Queue payloads, stored-hash reuse and scheduling-only
requeues are unchanged.

## Validation

`go test ./... -count=1`, `go test -race ./... -count=1`, `go vet ./...` and
`go build ./cmd/magpie` pass. The checker and checker-config PostgreSQL tests
pass with `MAGPIE_TEST_POSTGRES_DSN` set. Without the shared pick,
`TestOwnersWithSameJudgeSetShareOneRequestPerProtocol` fails with five
assignments where two are expected.
