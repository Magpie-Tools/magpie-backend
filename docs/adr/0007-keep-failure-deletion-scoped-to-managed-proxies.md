---
status: accepted
---

# Keep failure deletion scoped to managed proxies

Each workspace chooses whether its automatic failure action pauses or deletes a managed proxy, with pause remaining the default. Deletion removes that workspace's managed proxy, failure streak, and tag assignments, while preserving other workspaces' management of the shared route. It follows manual deletion and keeps no deletion tombstone, so a later import or scrape may add the route again with a fresh failure streak.

## Considered options

Remembering deleted routes would prevent repeated scraping from adding a failing route again, but would turn deletion into a suppression policy with its own lifetime and ownership rules. We chose the existing deletion semantics so imports and scrapers continue to treat an unmanaged route as eligible for management.

## Consequences

Changing the failure action leaves existing paused and archived managed proxies untouched. Delete requires a subsequent failed check and does not delete proxies from saved failure streaks during startup. Pause keeps its existing startup enforcement. Automatic deletion applies only while the managed proxy is active, including when a user changes its lifecycle during an in-flight check.

The action adds no operations to ordinary checker cycles. See the
[operation budget and PostgreSQL measurements](../performance/failure-actions.md).
