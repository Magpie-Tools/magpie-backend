---
status: accepted
---

# Evaluate alerts from existing workspace measurements

Pool availability follows each rotator's full current eligibility policy. Rolling success rate and latency instead describe attributed workspace checker attempts, restricted to the rotator's upstream protocol over TCP, because selecting only currently healthy routes would hide the failures that caused an incident. Exact historical membership of differently filtered pools is unavailable; adding that attribution would require extra storage work for every check. The API and UI name these as checker measurements, and rotators sharing a protocol share those measurements.

Evaluate existing projections and a shared 15-minute history aggregate in a background job. This adds zero cryptographic operations, JSON encodes, Redis commands, or database queries per proxy check. Historical attempts retain their own workspace verdicts, including previous settings and routes subsequently paused or deleted. Physical orphan cleanup waits until a route has no checks inside that window, so deleting its final managed association cannot erase recent failures. This retains ownerless route records briefly; managed ownership, capacity, and rotation still end immediately.

Limit each evaluation pass to 45 seconds and use four PostgreSQL workers. Process all enabled rules in a workspace as one batch, ordered by the workspace's latest attempt. Persist one rule's attempt timestamp before measurement so a canceled pass or leadership change does not always restart at the lowest workspace ID. Successful state writes update all observed rules' attempt timestamps in the same statement as their measurements. This avoids a separate scheduling write and transaction for every rule. Attempt timestamps remain separate from successful observations and never advance breach or recovery timers.

Whole-workspace usable-route rules share grouped reads of active ownership and current checker evidence across the pass. Join attributed evidence on both workspace and physical route; joining only on a shared physical route can produce a quadratic cross-workspace join. Keep legacy generation-zero evidence and overall status as a separate compatibility branch. Counts preserve the current checker default/override keys, ownership, distinct physical routes, and freshness policy used by individual workspace measurements. Pool filters still use the rotator's exact eligibility policy, once per distinct filter in that workspace batch.

Budget the shared history scan at 15 seconds, shared workspace route counts at five seconds, each distinct pool-count measurement at five seconds, and a workspace's pool measurements at ten seconds in total. Scheduling and observation transactions each have a three-second budget. A failed measurement produces Unknown with a fresh state budget. History cleanup has a separate ten-second budget.

Persist each workspace's rule states, incident transitions, and delivery records in one transaction. Lock its workspace and current rule rows, then recheck each rule revision and the checker generation before applying observations. Insert incidents and outbox records in bounded batches, and update rule states and incident closures with one PostgreSQL statement per workspace. Stale, disabled, deleted, and foreign rules cannot consume another rule's measurement. A failed outbox write rolls back the entire workspace observation. Use fenced claims for multi-instance delivery. Missing measurements interrupt breach/recovery timers and cannot close an incident. Two minutes of observed breach or recovery trigger one corresponding notification; editing configuration closes incidents without reporting recovery. Destinations are workspace-owned, administrator-managed, encrypted with the existing stable secret key, and write-only through the API.

Use ten delivery workers per backend instance and claim only available worker slots. Refill slots as messages finish, including when an earlier event's completion makes its recovery message eligible. Poll every five seconds while no work is eligible or after a claim error. Slow receivers occupy their own slots. Claims and ordering fences remain database-owned.

The alert page loads rules and history independently of scope metadata. A separate workspace-scoped metadata query selects rotator IDs, names, and upstream protocols without counting routes or reading credentials. The page caches those details across automatic refreshes, mutations, and history pagination; workspace changes and explicit refresh reload them.

## Cadence regression

`TestAlertsScaleCadencePostgres` runs six consecutive minute-spaced observations of 2,000 workspaces with 100 enabled usable-route rules each. The first three observations have zero proxies and must open exactly 200,000 incidents. The next three use one physical route with independent current evidence in every workspace and must recover every incident. Each pass must evaluate all 200,000 rules within 45 seconds without errors, and the final state must contain no duplicate incidents or unfinished timers. The optional destination variant queues one opening and one recovery record per incident without sending any messages.

Measured on 2026-10-10 with PostgreSQL 17 restricted to two CPUs, `GOMAXPROCS=2`, four evaluator workers, and a 32-connection test pool:

| Pass | Transition | Without destinations | One destination per rule |
| --- | --- | --- | --- |
| 0 | Start breach | 16.511 s | 18.833 s |
| 1 | Continue breach | 17.997 s | 19.120 s |
| 2 | Open 200,000 incidents | 20.001 s | 27.440 s |
| 3 | Start recovery | 21.640 s | 19.780 s |
| 4 | Continue recovery | 19.416 s | 20.888 s |
| 5 | Recover 200,000 incidents | 24.587 s | 29.485 s |

The destination variant also verified exactly 400,000 durable notification records. This workload validates rule processing and transition throughput; proxy history volume, projection size, notification fanout, and concurrent database load still determine a deployment's capacity. Run the same regression against a disposable database on the target hardware, then measure representative production data. No change to the one-minute interval or 90-second observation-gap reset is needed.

```sh
MAGPIE_TEST_ALERT_SCALE=1 \
MAGPIE_TEST_ALERT_SCALE_DESTINATIONS=1 \
MAGPIE_TEST_POSTGRES_DSN='host=127.0.0.1 port=5432 user=alerts_test password=... dbname=alerts_test sslmode=disable' \
GOMAXPROCS=2 go test ./internal/database -run '^TestAlertsScaleCadencePostgres$' -count=1 -v
```

Without `MAGPIE_TEST_ALERT_SCALE_DESTINATIONS=1`, the test reproduces the zero-destination workload. Normal test runs skip this scale regression. PostgreSQL tests also cover the full per-workspace fanout of 100 rules and ten enabled destinations, constant batch statement counts, outbox rollback, and current/legacy route-count equivalence.
