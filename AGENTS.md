# magpie-backend agent instructions

The Magpie workspace keeps its binding rules in the workspace-level `AGENTS.md`,
one directory above this repository. Its **Proxy checker hot path** section
applies to every change here and is not repeated in this file. Read it before
touching `internal/jobs/checker`, `internal/jobs/queue/proxy`,
`internal/rotatingproxy` or `internal/checkpace`.

Start with the [architecture walkthrough](docs/architecture.md) and the
[glossary](CONTEXT.md). Decisions are recorded in [docs/adr](docs/adr/); the
staged package extraction is
[ADR 0015](docs/adr/0015-extract-feature-packages-behind-architecture-tests.md).

## Package map

Every package has a doc comment in its `doc.go` (or beside its main type); read
that first. Paths are relative to `internal/`.

| Layer | Packages | Owns |
| --- | --- | --- |
| Entry and composition | `app`, `app/bootstrap` | Flags, one-shot commands, start and stop order of every routine. |
| Transport | `app/server`, `graphql`, `api/dto` | The central REST route table, GraphQL schema, request and response shapes. |
| Hot path | `jobs/checker`, `jobs/queue/proxy`, `rotatingproxy`, `checkpace` | Checker workers, the Redis proxy queue, rotating listeners, allowance cadence and the check usage meter. Imports are pinned. |
| Background jobs | `jobs/runtime`, `jobs/scraper`, `jobs/queue/sites`, `jobs/maintenance`, `jobs/checker/judges` | Statistics admission and ingestion, rollups, scraping, cleanup, the judge cache. |
| Features | `alerts`, `mail` | Alert rules, evaluation, incidents and delivery, below the core, which calls it inside its transactions; the email outbox with its delivery and maintenance routines. |
| Core persistence | `database` | Routes and managed proxies, statistics and latest evidence, read models, dashboards, rotators, checker settings, usage accounting. |
| Platform | `platform/pg`, `platform/schema` | Generic PostgreSQL helpers and every database connection (the GORM pool, the rebuild command's connection, the native COPY connection); the ordered schema steps, migrations and seed rows. |
| Evidence | `evidence` | SQL builders for a workspace's current checker evidence, alive counts, and a rotator's pool-eligibility query and cache key. Runs on the caller's handle. |
| Runtime configuration | `config`, `settings`, `checkerconfig` | Instance settings, workspace settings application, compiled checker snapshots. |
| Shared models | `domain` | GORM models and their hooks. No queries. |
| Clients and helpers | `abuseipdb`, `ipgeolocation`, `geolite`, `blacklist`, `auth`, `security`, `support`, `support/reputation`, `instanceid`, `app/releases`, `app/version` | External API clients, JWTs, credential encryption, Redis client and leader locks. |
| Test kit | `testkit/db` | `OpenSQLite` and `OpenPostgres` test databases migrated through `platform/schema`, and generic row builders. Imported only by `_test.go` files. |
| Guardrails and tools | `archtest`, `tools/movecheck`, `tools/onboarding`, `tools/sqllog` | Architecture tests and their data, run under `go test ./...`; the rename-only move check; the onboarding measurement; the SQL-log digest. |

## Where new code goes

- **A request handler** goes in `app/server` (REST) or `graphql`. Register REST
  routes in the central route table. Handlers stay thin and call a feature
  package or `database`.
- **Persistence for a feature that already has its own package** goes in that
  package. Feature packages never import `app/server`, `graphql`, `app` or
  `app/bootstrap`.
- **New persistence in `database`** is justified only when the code must run
  inside one of the core's transactions (statistics ingestion, route import,
  lifecycle, cleanup, read-model refresh). `archtest` holds the package's line
  ceiling; a change that adds core code raises that number in the same diff.
- **A background routine** goes in `jobs/runtime` unless its feature package
  already owns one. Start it from `app/bootstrap`, never from `init()`.
- **Shared models** go in `domain`. Do not combine a model relocation with a
  query move.
- **A transaction** is begun and committed by exactly one package. Outside
  `database`, a function that receives a `*gorm.DB` runs on its caller's handle
  and must not begin, commit or roll back.
- **Package-level state**: do not add package-level variables. Sentinel errors,
  compiled regular expressions, embedded files, Redis scripts and Prometheus
  collectors are the exceptions.
- **Database and Redis connections** are created only in the places `archtest`
  lists.
- **Hot path**: anything that changes the per-check cryptographic operations,
  JSON encodes, Redis commands or database queries needs the written accounting
  the workspace `AGENTS.md` requires. The current counts are in
  [docs/performance/hot-path-operation-baseline.md](docs/performance/hot-path-operation-baseline.md).

## Guardrails

`internal/archtest` enforces the rules above. Its data lives in
[internal/archtest/rules](internal/archtest/rules/):

| File | Rule |
| --- | --- |
| [imports.txt](internal/archtest/rules/imports.txt) | The allowed internal imports of every package. This is the import table: change it in the same diff as a new import. |
| [layers.txt](internal/archtest/rules/layers.txt) | Forbidden dependency directions and the shrinking list of existing violations. |
| [hotpath_pins.txt](internal/archtest/rules/hotpath_pins.txt) | The exact import sets of the hot-path packages. |
| [core.txt](internal/archtest/rules/core.txt) | The `database` line ceiling and importer list. |
| [transactions.txt](internal/archtest/rules/transactions.txt) | Functions temporarily allowed to break the transaction rule. |
| [connections.txt](internal/archtest/rules/connections.txt) | The sites allowed to create connections. |
| [state.txt](internal/archtest/rules/state.txt) | Package-level variable counts and `init()` functions that still start goroutines. |
| [docs/performance/README.md](docs/performance/README.md) | The performance-notes index. Notes marked historical are not checked. |

An allowlist only shrinks. Do not add an entry to get a change through; if a
rule blocks a change, the change needs a different shape or an ADR.

## Required checks

Run from the repository root before every merge:

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build -o /dev/null ./cmd/magpie
make test-integration
```

`make test-integration` starts disposable PostgreSQL and Redis containers on
free ports, runs the suite against them and removes them. It never touches an
existing container.

A change that moves code between packages is verified as a pure move with
`make move-check BASE=<revision>`: every declaration must be token-identical
to one in the base once package qualifiers and export case are normalized.

A change that moves a statement to another handle, or moves a function that
runs inside another package's transaction, also needs the SQL-log comparison in
[docs/onboarding/sql-log.md](docs/onboarding/sql-log.md). Core changes also run
the scale tests on matched hardware.

Package extraction is measured against a fixed onboarding task set; the
procedure, the checkpoints and the stop rule are in
[docs/onboarding](docs/onboarding/README.md).
