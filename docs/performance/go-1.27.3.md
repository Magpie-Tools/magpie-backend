# Go 1.27.3 security patch and component measurements

Validated on 22 October 2026. The module and Docker builder now pin Go 1.27.3.
The patch was released on 19 October; the advisory was published on 21 October.
[GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809) affects
`crypto/x509.Certificate.Verify` before Go 1.27.3 in the 1.27 series. It adds a
certificate public-key denylist for trust-store cases that may otherwise retain
distrusted intermediates. The [Go release history](https://go.dev/doc/devel/release#go1.27.3)
identifies the security fix.

The recorded Go 1.27.3 runtime and compiled benchmark build information match the
pin. The fresh Linux/amd64 symbol-level scan with `govulncheck` 1.8.0 exits zero
with zero reachable vulnerabilities. It still reports GO-2026-5932 only at
module level for unused `x/crypto/openpgp` packages. See
`.codex-tmp/release-remediation-20261020/toolchain-20261022/security-final.json`
for the scanner configuration, commands, source hashes and assessment. Full Go
normal/race/vet/build and recovery/soak validation must run again with the patch;
these component measurements do not replace them.

## Comparison method

Both versions compile the same current application, dependencies, tests and
embedded assets. A temporary source snapshot supplies the old baseline with
only the `go.mod` directive changed from `1.27.3` to `1.27.2`. The source-parity
manifest hashes all 524 copied files and 468 compilation inputs. Exact
`GOTOOLCHAIN=go1.27.2` and `GOTOOLCHAIN=go1.27.3` pins prevent automatic compiler
selection. The four existing queue/checker test binaries were compiled once,
then their hashes were verified before use. No canonical application or test
source changed for this measurement.

Each of the six required workloads and two TLS supplements has five one-second
samples per toolchain, 80 successful benchmark commands in total. Version order
alternates within each workload and reverses on the next sample. All commands
run sequentially. `GOMAXPROCS=4` and CPU affinity 0, 2, 4 and 6 place clients on
four physical performance cores of this Intel Core i7-12700H host. CPU affinity
does not reserve those cores; the host is shared and has no hard client-memory
limit. Clock frequency, scheduling and thermal state remain sources of noise.

The dedicated Redis 7.4.8 container uses CPU 8, a one-CPU quota, 256 MiB memory
with no additional swap, and no persistence. It binds only loopback port 32775.
The queue benchmarks use database 15, verified empty before and after every
sample. They seed a ready route per iteration outside the timed loop and use
two workspace owners. Decode workloads separately cover one and eight owners.
At `GOMAXPROCS=4`, the concurrent queue benchmark uses 32 workers. HTTP benchmarks
use four parallel workers and the existing 8 KiB body limit.

The TLS supplement is temporary measurement code, separate from application
source. It uses one identical ECDSA P-256 root/intermediate/leaf chain for both
binaries. `Certificate.Verify` must return one trusted chain of three
certificates. The fresh HTTPS case uses a local server, a 2 KiB response, normal
verification against that root, disabled keepalives and disabled session
resumption. Every response asserts a newly verified three-certificate chain.
No certificate-verification bypass or global root-store change is used.

## Results

These are medians from five samples. Concurrent ns/op is total wall time divided
by completed operations, the reciprocal of aggregate throughput. It is not an
individual worker's request latency.

| Workload | Go 1.27.2, µs/op | Go 1.27.3, µs/op | Median time change | Bytes/op, old / new | Allocs/op, old / new |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plaintext decode, one workspace | 1.282 | 1.275 | -0.5% | 920 / 920 | 5 / 5 |
| Plaintext decode, eight workspaces | 2.358 | 2.296 | -2.6% | 5,321 / 5,321 | 8 / 8 |
| Redis cycle, one worker | 122.922 | 129.308 | +5.2% | 4,002 / 4,003 | 53 / 53 |
| Redis cycle, 32 workers | 82.908 | 84.415 | +1.8% | 4,133 / 4,133 | 53 / 53 |
| HTTP check, 2 KiB | 25.708 | 25.252 | -1.8% | 15,806 / 15,806 | 93 / 93 |
| HTTP check, 64 KiB rejected at 8 KiB | 53.718 | 52.896 | -1.5% | 91,938 / 91,938 | 105 / 105 |
| X.509 verify, three-certificate chain | 115.108 | 115.356 | +0.2% | 2,664 / 2,856 | 54 / 57 |
| Fresh verified TLS + HTTP, 2 KiB | 423.723 | 420.892 | -0.7% | 144,645 / 144,860 | 1010 / 1013 |

All old/new timing ranges overlap. The one-worker Redis median increases 5.2%,
with ranges of 88.4–140.5 µs and 87.7–149.0 µs; the 32-worker increase is 1.8%,
with ranges of 44.6–88.2 µs and 44.5–96.9 µs. This is an observed median increase,
not evidence of a stable capacity regression. If the one-worker median were
repeatable and queue work were the bottleneck, its reciprocal would imply about
4.9% less queue capacity. These short shared-host samples cannot establish that
premise. Queue allocations remain exactly 53 per cycle in every sample.

Likewise, the lower decode and HTTP medians do not establish a speedup. Fresh
verified HTTPS ranges from 224.5 to 475.0 µs on the old version and 222.8 to
554.3 µs on the patch. The patch is a required trust correction; the measurements
make no production capacity or throughput-improvement claim. Retain queue wait,
Redis CPU and sustained checker-throughput checks in the deployment soak.

## Operations and certificate verification cost

For a current plaintext payload with a valid stored hash, the queue application
path is identical under both compilers:

| Operation per ready dequeue / normal requeue cycle | Both versions | Added by this patch |
| --- | ---: | ---: |
| Credential encryption/decryption or nonce generation | 0 | 0 |
| Route fingerprint calculation | 0 | 0 |
| JSON encodes / decodes | 0 / 1 | 0 / 0 |
| Queue-issued Redis wire commands | 3 | 0 |
| Database queries | 0 | 0 |
| Payload writes | 0 | 0 |

The warm wire sequence is `EVALSHA`, interval `GET`, then fenced-completion
`EVALSHA`, three round trips. On the ordinary valid-head leased path, the pop
script makes six Redis data calls and completion makes four. The interval GET
is separate. Initialization, recovery, stale heads, exceptional legacy payload
migration and long-running lease renewal are outside this count. Existing
regressions pass under both binaries and prove plaintext default storage,
stored-hash reuse without an encryption key, payload preservation and the
three-command cycle. Credential encryption remains opt-in.

A universal zero-added-crypto claim would be wrong. The Go standard-library diff
adds a SHA-256 hash of each certificate's SPKI plus a hexadecimal conversion when
a candidate chain reaches the new denylist check. The successful single-chain
fixture adds three SHA-256 operations and three hex conversions per verify.
Measured X.509 allocations increase from 54 to 57 and from 2,664 to 2,856 bytes,
exactly 192 additional bytes. Its median time change is +0.2%, with overlapping
112.9–117.7 µs and 108.9–118.9 µs ranges.

Plain HTTP and queue operations do not verify a certificate chain and add none
of those hashes. The production TCP checker explicitly keeps
`InsecureSkipVerify=false`; HTTP/3 defaults to verification too, and SMTP TLS is
also verified. A fresh verified connection therefore incurs the new trust
check. Connection reuse can amortize it across checks, so it is not necessarily
paid once per proxy check. No route hash is recomputed, and this patch does not
change the Redis credential trust boundary. The written reason for the extra
certificate work is the mandatory GO-2026-6809 trust mitigation.

## Evidence and cleanup

The workspace's `toolchain-20261022/` evidence directory retains source parity,
compiler environments/build information, exact benchmark commands, all 80 logs,
raw samples and median/range/allocation summaries. It also retains the temporary
TLS source and certificates, the standard-library diff and both versions of the
four existing hot-path regressions. `benchmarks-ready.json` records their hashes
and references the separate security scan.

Database 15 was empty after every benchmark; databases 0, 14 and 15 were empty
before cleanup. Database 14 was used only by the existing real-Redis regression
fixture. The owned container was stopped successfully and removed. Canonical
Redis on port 32771 and the retained release fixture were not used or flushed.
No release images, publication or other item-4 work was performed.
