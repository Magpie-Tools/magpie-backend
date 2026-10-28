# Workspace list count batching, 21 October 2026

Listing eight workspaces sharing a million routes took 28.574 seconds during
the isolated baseline's startup load, close to the 30-second HTTP write deadline.
`ListWorkspaces` performed two exact counts and a table-existence check for each
membership. It now checks the table once and obtains active and stored counts
for all memberships in one grouped query. Empty workspaces retain zero counts;
membership ordering, permissions and subscription fields are unchanged.

The count comparison used the same restored local database, with autonomous
backend jobs stopped, PostgreSQL limited to four CPUs and 4 GiB, 1,000,001 routes
and 8,000,008 ownership rows. Two routes per workspace were paused or archived.
The sixteen previous count calls took 4.701 seconds, and the grouped count took
1.556 seconds. Every workspace returned 999,999 active and 1,000,001 stored
routes in both forms. Calls include the local Compose/psql invocation overhead;
the comparison ran sequentially and does not establish latency under sustained
load. Repeated API and k6 measurements remain necessary.

Evidence is retained in the workspace's private remediation directory as
`workspace-list-counts-scale.json` and `workspace-list-counts-scale.log`.
`TestWorkspaceListCountsSharedRoutesAndEmptyMemberships` covers shared routes,
all three lifecycle states, empty membership inventory, default ordering and
nonmember isolation. No operations are added to proxy checks, ingestion,
requeueing, credential handling or route hashing.
