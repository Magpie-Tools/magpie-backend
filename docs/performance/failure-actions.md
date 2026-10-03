# Proxy failure-action operation budget

The workspace failure action travels through the existing checker-settings
cache and failure event. Adding the field adds no query to workspace refresh
and does not change the Redis payload format, which contains workspace IDs.

| Added operation per ordinary proxy check | Count |
| --- | --- |
| Cryptographic operations, including route hashes | 0 |
| JSON encodes | 0 |
| Redis commands | 0 |
| Database queries | 0 |

Normal requeues still update scheduling only. A failure action changes active
ownership, so the existing ownership-change path may rewrite a shared route's
payload or remove an inactive route from the queue. That work already occurs
for Pause and is required for Delete as well.

Automatic deletion replaces the active-row update with an active-row delete
and replaces the filter-index upsert with a filter-index delete. Tag assignments
cascade in the database. Both actions refresh the same source statistics and
active-route usage, then check for remaining active management. Deletion does
not load managed credentials, calculate a route hash, or introduce another
database query into that lifecycle transition. Its transaction preserves the
workspace boundary and rolls back if read-model or usage updates fail.

## PostgreSQL validation

On 2026-10-03, `TestFailureActionsPostgresLifecycleAndOperationBudget` ran
against local PostgreSQL 17. Each action used an isolated schema with 10,000
managed routes, a scrape source, filter indexes, usage records, and tag
assignments. The test applied 32 lifecycle transitions per action.

| Action | Traced SQL statements | Statements per transition | Mean time per transition |
| --- | --- | --- | --- |
| Pause | 384 | 12 | 32.14 ms |
| Delete | 384 | 12 | 30.53 ms |

Statement counts include table-existence checks and exclude transaction
boundaries. Each transition uses one transaction. These local timings describe
lifecycle cleanup with source-statistics aggregation, rather than overall
checker throughput. The measured query count is identical for both actions.

The test also verifies source counts, filter indexes, tag cascades, capacity
usage, and reimport behavior. Run it with `MAGPIE_TEST_POSTGRES_DSN` set:

```sh
go test ./internal/database -run '^TestFailureActionsPostgresLifecycleAndOperationBudget$' -count=1 -v
```

The existing queue tests remain required. A current plaintext payload must
reuse its stored hash without a cipher, and an ordinary requeue must leave the
payload untouched.
