# Tag checker settings

This design was confirmed after the grill-with-docs interview and is implemented across the backend, frontend, user guides, and upgrade tooling documentation.

## Settled behavior

Checker Settings has a selector for Default and the active workspace's tags. Default is the baseline for every managed proxy in that workspace. A tag with no checker rule changes nothing. Rules use tag identity, so renaming or recoloring a tag retains its rule; deleting the tag removes its rule.

HTTP, HTTPS, SOCKS4, and SOCKS5 each have an enabled state. Default and each tag profile have one shared transport, timeout in milliseconds, and retry count for all enabled checks. This replaces the original per-protocol design at the user's request. TCP supports all four proxy protocols; QUIC and HTTP/3 accept HTTP and HTTPS with HTTPS judges.

Start from Default and apply every matching rule in increasing priority order. The highest-priority tag is at the top and applies last. A separate Tag priority popup uses draggable rows and up/down controls like the table columns popup. Apply order updates the editor draft; Cancel discards popup edits. The main Save action commits settings and order together.

| Mode | Protocol selection | Shared settings |
| --- | --- | --- |
| Replace | Use only the rule's selected protocols. | Override explicitly set fields for every enabled check. |
| Add | Merge the selected protocols into the current set. | Override explicitly set fields for every enabled check, including existing protocols. |
| Remove | Exclude the selected protocols. | Retain the current transport, timeout, and retries. |

Transport, timeout, and retries inherit independently from Default and preceding matching rules. Re-enabling a protocol uses the current shared values. An Add rule with no selected protocols can override only the shared fields.

For example, Default enables SOCKS5 with TCP, 7500 ms, and two retries. An Add rule enables HTTP with 5000 ms and one retry. Both HTTP and SOCKS5 now use TCP, 5000 ms, and one retry. A higher rule can override only timeout for both checks.

Judges, HTTPS judge routing for SOCKS, automatic failure handling, Pause/Delete, and the failure threshold remain workspace-wide. These controls are not part of a tag checker rule.

An empty effective protocol set skips checking without pausing or deleting the managed proxy and without increasing its failure streak. The managed proxy remains active and continues to consume workspace capacity. Changing settings or tags affects the next scheduled check; an already-running check completes with the settings it started with. Editing settings does not itself reset a failure streak.

The editor retains drafts while switching between Default and tags. One Save changes action commits all edited checker profiles and their priority order together. Failed saves retain the drafts and do not mark them as saved.

## Health and rotation

Managed-proxy health reflects that workspace's current effective checker settings, including protocol, transport, timeout, retries, and judge validation. A result from another workspace or an obsolete configuration cannot make the managed proxy alive. Old results remain available as history. A current configuration with no matching result has unknown health; an empty check selection provides no evidence of liveness.

Changing a configuration invalidates only the evidence whose effective check settings changed. An in-flight result for the old configuration remains historical and cannot overwrite current health. Legacy statistics have no reliable transport or configuration attribution, so they remain historical evidence rather than being relabeled as current TCP checks.

Rotating listeners require successful evidence for the transport they actually use to connect upstream. Current rotators connect upstream over TCP, so a successful QUIC check cannot qualify a proxy for TCP rotation. Health and uptime filters use evidence applicable to the selected workspace and check configuration. See [ADR-0011](../adr/0011-scope-managed-proxy-health-to-effective-checker-settings.md).

## Runtime constraints

Every enabled protocol uses its effective profile's shared timeout and retry count. Requests shared across workspaces must not borrow the largest budget from another workspace. Sharing a physical request requires the same judge, proxy protocol, transport, timeout, and retry count; response validation remains attributable to the workspace's judge expression.

Checker rules and relevant tag assignments come from a complete in-memory snapshot. Startup loading, post-commit refresh, cross-instance updates, and reconciliation occur outside individual checks. Imports, manual tag changes, source tagging, rule changes, and deletion all update this state. See [ADR-0010](../adr/0010-resolve-tag-checker-settings-from-runtime-snapshots.md).

Tag-rule resolution must add zero cryptographic operations, JSON encodes, Redis commands, or database queries per proxy check. Queue payloads continue to carry the stored route hash, and ordinary requeues update scheduling only. Measure rule-resolution CPU, allocations, and assignment-cache memory separately. Count and measure health-persistence costs as well; any additional operations require representative throughput measurements and a written reason before merging.

## Compatibility and validation

Existing workspaces begin with their current shared checker values and no tag rules. Unrelated settings saves and older clients that omit new fields must preserve tag rules and shared profile settings. Existing workspace permissions continue to apply: viewer or higher can read, and operator or higher can save.

Validate combinations in the editor and API, and preserve existing retry semantics. Cover protocol merging, inheritance, priority conflicts, tag lifecycle, workspace isolation, actual timeout and retry execution, empty protocol sets, drafts and save failures, health isolation, stale evidence, and TCP rotation eligibility.

Retain the queue tests proving that current plaintext payloads reuse their stored hash without invoking the cipher and that ordinary requeues do not rewrite payloads. Run `go test ./... -count=1`, `go test -race ./... -count=1`, `go vet ./...`, and `go build ./cmd/magpie` from the backend. Validate the Angular changes with its relevant tests and production build. Update user documentation and coordinated upgrade notes in magpie-docs and magpie.

## Implementation

The REST settings payload has `checker_settings.defaults` and an ordered
`checker_settings.rules` list. GraphQL exposes `checkerSettings` with protocol-name
lists. Both validate the complete profile update and tag ownership in one
workspace transaction. Scalar fields omitted by older REST or GraphQL clients
retain the stored values. Existing scalar defaults expand lazily to a shared
profile, and existing queue payloads remain compatible.

`internal/checkerconfig` compiles immutable workspace snapshots and deduplicates
identical effective plans. Workers read protocol keys, eligible judges, captured
headers, and budgets from one snapshot for the entire cycle. Cross-instance
Redis notifications trigger refresh, and minute reconciliation repairs missed
notifications. A failed refresh retains the preceding complete runtime snapshot.
The committed workspace is marked pending until its SQL projection refreshes;
health reads suppress old evidence while pending. Retention does not prune valid
pointers while their projection is pending. Reconciliation uses persisted default
keys and sparse overrides as its baseline, so missed notifications and reversions
cannot leave obsolete overrides active. Workspace revision watermarks and a
coalesced per-route change journal select the assignments and projection rows to
read. Unchanged cached reconciliation reads no route data, and column-only saves
skip checker notification entirely. Immutable map shards preserve the preceding
snapshot while copying only changed shards. Startup and shared Default, judge,
header, or SOCKS-routing changes still load the full workspace. It rewrites only
changed routes and associated source summaries. Unrelated classification tags
skip invalidation.
Ownership deletion records affected routes before assignments disappear. Removed
memberships retain recovery tombstones until retention maintenance advances a
full-refresh watermark and expires them. Missed deletion messages and subsequent
reimports cannot preserve checker settings from a previous membership.
Read models retain committed-key evidence during pending edits; public readers
mask it until the projection transaction completes.

Small source and route queries aggregate evidence only after restricting the
workspace and candidate proxy. Dashboard recent checks select limited candidates
from an indexed health projection before fetching their evidence. Dashboard
cache entries record the checker
generation and validate it against PostgreSQL on reads and before publishing a
refresh. An older query cannot restore obsolete health after invalidation.

Workspace defaults hold protocol configuration keys. `proxy_checker_plans`
stores only overrides that differ from Default, including explicit removals.
One physical statistic carries each participating workspace's verdict and key.
Latest pointers use workspace, configuration, route, and protocol as their key;
new attributed events do not also write redundant global latest or overall
rows. Current-health queries and TCP rotator selection match the effective keys.

See [performance measurements](../performance/tag-checker-settings.md) for the
operation budget, snapshot memory, and persistence cost at one and eight
participating workspaces.

The current REST and GraphQL profiles contain `protocols`, `transport`, `timeout`, and `retries`. Tag rules add a tag identity and mode, with optional shared overrides. Earlier persisted per-protocol draft JSON remains readable. Its first enabled protocol in HTTP, HTTPS, SOCKS4, SOCKS5 order supplies Default's shared fields; each rule uses its first explicit value for each field in that order. If those fields create an unsupported SOCKS/transport combination, conversion uses TCP for the shared transports. Subsequent writes use the shared format. Validation examines at most 48 reachable selection/transport states across any number of rules, outside the checker loop.
