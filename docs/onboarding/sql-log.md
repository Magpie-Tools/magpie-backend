# SQL-log comparison

A package move must not change which statements run, their order, or the
transaction they run in. Statement-count tests catch a changed count. The
SQL-log comparison also catches a statement that moved to another transaction
or another handle.

## When it is required

- A change moves a statement to another handle. "Same handle" means the same
  handle identity and transaction scope, not merely the same database.
- A change moves a function that runs inside another package's transaction
  (`evidence`, `alerts`, `workspace`, the read-model pilot, the hot-path
  ownership changes).

A leaf move, where nothing inside the core calls the moved code and it runs on
the same handle, is verified by a rename-only diff and the existing suites
instead.

## Procedure

```bash
git switch --detach BASE && make sql-log OUT=/tmp/before.sqllog
git switch CHANGE        && make sql-log OUT=/tmp/after.sqllog
diff /tmp/before.sqllog /tmp/after.sqllog
```

`scripts/sql-log.sh` starts its own PostgreSQL 17 with `log_statement=all` and
the logging collector, and its own Redis, on free ports. It runs the tests in
[scripts/sql-log.tests](../../scripts/sql-log.tests) one package at a time,
then pipes the server log through `internal/tools/sqllog`. Both containers are
removed on exit.

The digest lists every distinct transaction: its statements in order, and how
often that exact sequence ran. Statements are grouped by backend process and
virtual transaction ID, so an explicit transaction is one sequence and an
autocommit statement is its own. An empty `diff` means the same statements ran
in the same per-transaction sequences with the same multiplicity.

Normalized before comparison: schema names with a time suffix, quoted
literals, numbers of nine or more digits, placeholder numbers, savepoint names,
the clause order of `CREATE TABLE` (GORM emits constraints in map order), and
the length of placeholder lists and repeated `VALUES` rows, which covers
variable `IN` lists. [scripts/sql-log.ignore](../../scripts/sql-log.ignore)
drops the driver's connection liveness probe and nothing else.

Take both digests on the same day: monthly partition names are part of the
statements.

## Stability proof

The comparison is only meaningful if two runs of one revision agree. Three
consecutive runs at `3f66f5d` (the tree this procedure was introduced on)
produced byte-identical digests:

```
# 7855 transactions, 541 distinct sequences, 12182 statements
```

Re-prove this with two runs of the base revision whenever the test list
changes. A test is listed only if its statements are deterministic. Tests that
race workers against each other, or that depend on timing, are left out; their
own assertions cover them.

## Scope

The log covers PostgreSQL only. Tests that run on SQLite issue no statements
here. The listed tests cover current evidence and rotator pools, alert
measurement and transitions, the statistics transaction and check usage,
import and reimport, lifecycle and failure actions, read-model refresh and
rebuild, the development seed, and the checker's failure-streak writes.
