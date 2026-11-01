---
status: proposed
---

# Scope shared-route health to checker configuration

ADR 0011 keys current evidence by workspace, configuration key, route and
protocol, and failure streaks live on each `user_proxies` row. Work per check
therefore grows with the number of owners. Scraped public routes are the ones
thousands of workspaces share, and most of them are dead. For a route with N
owners, one cycle currently produces:

- N workspace IDs in the Redis payload, decoded on every dequeue, plus N
  `domain.Workspace` copies in `refreshProxyWorkspaces` and again in
  `buildRequestAssignments`;
- N `CheckEvidence` entries per statistic, JSON-encoded into the stream,
  decoded by the consumer and stored as `jsonb` in every history row;
- N × protocols `proxy_latest_statistics` upserts, because the conflict key
  includes `workspace_id`;
- N `user_proxies` updates for each failed cycle until the streak saturates.

[Persistence measurements](../performance/tag-checker-settings.md) already show
attributed persistence falling from 14,978 to 4,532 physical events per second
when one result serves eight workspaces. At 1,000 owners the history row alone
carries tens of kilobytes of evidence.

## Proposal

Configuration keys are content hashes of everything that decides a verdict:
protocol, transport, timeout, retries, the judge list with each regex, and
headers. Two workspaces with the same key receive the same verdict from the same
physical response. Since judges are now chosen once per judge set and cycle,
they also receive the same judge. Health can therefore be stored once per key
instead of once per owner:

1. History evidence stores one `{config_key, alive}` entry per distinct key.
2. `proxy_latest_statistics` is keyed by route, protocol and configuration key.
   A workspace resolves its key from its default plan or the sparse
   `proxy_checker_plans` override, as health queries already do.
3. A `proxy_failure_streaks` table keyed by route and checker signature (the
   four protocol keys) replaces `user_proxies.consecutive_failures`. Owners with
   equal signatures see equal outcomes, so their streaks are equal. Each
   workspace's threshold is compared in memory from its checker snapshot, so
   threshold actions need no extra read.

With 1,000 owners on default settings, a cycle drops from about 1,000 evidence
entries, latest upserts and streak writes to between one and four of each.

## Consequences and open questions

- **Joining a route.** A workspace that adopts a route would inherit its shared
  streak, while today it starts at zero. An imported dead route could then be
  paused or deleted on its first failed check. This needs a decision: accept the
  inheritance, or store a per-owner baseline written only on adoption.
- **Isolation.** ADR 0011 prevents another workspace's verdict from qualifying a
  route. This change keeps that guarantee for different settings, but owners
  with identical settings would share one verdict. They already share one
  physical request today.
- **Migration.** Existing latest rows collapse to the newest row per key.
  Existing streaks migrate to the minimum across owners with the same signature,
  so no action fires earlier than it would have. Readers of workspace-keyed
  evidence (dashboards, rotators, alerts, cleanup and pruning) move to key
  lookups in the same release. Legacy history evidence stays readable.
- **Queue payload.** Owner fan-out in the Redis payload and the workspace copies
  are out of scope here. They can move to an ownership-set reference in a later
  ADR without changing the plaintext trust boundary in ADR 0002.

## Required measurements before acceptance

On the persistence benchmark from `tag-checker-settings.md`, extended to 1, 8,
100 and 1,000 owners: physical events per second, SQL statements and WAL per
batch, and history row size, before and after. Count added cryptographic
operations, JSON encodes, Redis commands and database queries per check. The
target is zero added operations in each category.
