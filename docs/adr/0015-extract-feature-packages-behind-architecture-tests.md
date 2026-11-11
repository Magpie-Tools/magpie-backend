---
status: accepted
---

# Extract feature packages in stages behind architecture tests

`internal/database` holds most persistence in one package of about 20,000 non-test lines. Newcomers and coding agents cannot tell from the package boundary which code a change may affect, and nothing stops a new dependency between unrelated features. Move peripheral features into packages with explicit, compiler-checked dependencies, one pure-move change at a time, and keep the code that shares transactions together.

This is onboarding and maintainability work, not capacity work. Performance neutrality is an acceptance condition of every step, proven by fresh evidence for that step. Historical measurements do not establish it. Owner fan-out ([ADR 0013](0013-scope-shared-route-health-to-checker-configuration.md)) and checker capacity work stay separate.

## Structure

Request handlers stay in `app/server` and `graphql` with one central route table. Feature packages never import transport, authentication middleware or bootstrap. `domain` keeps the shared GORM models, and a model relocation is never combined with a query move.

Base packages come first, each in its own change: `platform/pg` for generic SQL helpers and all production connection creation, `platform/schema` for ordered schema and migrations, `evidence` for current workspace evidence SQL and pool eligibility, and the shared test kit. Peripheral features follow in the order the compiler's dependencies allow: `mail`, `alerts`, `account`, AbuseIPDB and GeoLite persistence, `blacklist`, judge persistence, the scraper store, `devseed` and `workspace`. `database/ipgeolocation.go` and workspace usage and allowance accounting stay in the core because they run inside its transactions.

The remaining core keeps the name `database`. A further split, starting with a `readmodel` pilot, must show a compiler-verified dependency direction, a reviewed public API, unchanged SQL and statement counts, rollback and locking tests, and repeated scale comparisons on matched hardware covering throughput, latency, WAL, memory and projection lag.

## Rules

Each transaction is begun and committed by exactly one package. Other packages may expose functions that run on the caller's handle. Outside `internal/database`, a function with a `*gorm.DB` parameter may not call `Transaction`, `Begin`, `Commit`, `Rollback`, `SavePoint` or `RollbackTo`. A move that carries a violating function names it in an allowlist, and a later logic change reshapes it. Each cross-package transaction-scoped function has a rollback test that goes through the public entry point and checks that an outer rollback removes its writes.

Package-level singletons move unchanged with their package. No `init()` starts a goroutine, and a package's count of package-level variables cannot rise. The end state is one owner per stateful package, constructed in `app`, with explicit start and stop. The hot-path singletons (the two checker batchers and the check usage meter with its snapshot) move to explicit ownership only in dedicated hot-path changes that carry the full operation accounting, a composition test, a batching test and the stop order: checker workers, then the batchers, then the meter's final flush.

`internal/archtest` enforces these rules under `go test ./...`. Its rules and allowlists are data in `internal/archtest/rules`: the allowed-imports table, forbidden dependency directions, the pinned imports of the hot-path packages, the `database` line ceiling and importer list, the transaction allowlist, the connection-creation sites, per-package variable counts, package documentation and the documentation check. An allowlist only shrinks.

## Verification per change

A leaf move, where nothing inside the core calls the moved code and it runs on the same handle identity and transaction scope, is verified by a rename-only diff plus the existing suites. The moved code is byte-identical after `gofmt` once package qualifiers, and the export case of identifiers that cross the new boundary, are normalized.

A change that moves a statement to another handle, or moves a function that runs inside another package's transaction, also needs the SQL-log comparison: per-transaction statement sequences with multiplicity, proven stable on two runs of the same revision, with variable `IN` lists normalized.

Every change runs the statement-count tests, the hash-reuse and command-budget tests, and the race suite. Queue and checker changes need the workspace hot-path accounting. Core changes also run the scale tests on matched hardware.

## Checkpoints and the stop rule

A fixed set of onboarding tasks is measured with a written procedure after step 0, after `alerts`, and after the peripheral features. Extraction stops when a re-run shows no further drop for the task set. The procedure and every measurement are in [docs/onboarding](../onboarding/README.md).

## Consequences

Moves change import paths and package qualifiers and nothing else. They add no cryptographic operations, JSON encodes, Redis commands or database queries per proxy check. The cost is a transitional period in which some features still reach the core's shared handle, and allowlists that record the violations still to be removed.
