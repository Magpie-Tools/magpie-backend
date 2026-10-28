# Rotator current-evidence query scope

The million-route recovery rehearsal exposed a slow rotating-proxy list. Its
candidate count joined a global current-evidence view that first expanded each
latest result through shared route ownership. Eight workspaces shared the local
inventory, and the request exceeded the server's 30-second write timeout.

`buildAliveProxyQuery` now joins the selected workspace and its eligible latest
rows directly. Compiled workspaces use their own evidence namespace; generation
zero uses the legacy namespace. Sparse checker overrides still replace default
keys. Dirty workspaces, empty keys, paused/archived management, and non-TCP
compiled evidence remain ineligible. Reputation and uptime filters still apply
to the same candidate rows. Selecting a route still loads credentials from the
selected workspace.

The count remains an exact aggregate over eligible workspace inventory. It
does not load proxy credentials or build the full candidate list in Go.

## Regression coverage

`TestRotatorEvidencePreservesWorkspaceEligibility` checks shared ownership,
current and obsolete keys, sparse overrides, disabled protocol keys, dirty
configuration, lifecycle states, TCP and legacy transports, reputation, uptime,
and credentials belonging to different workspaces for the same route.

`TestRotatorEvidenceScopesSharedInventoryPostgres` populates eight owners per
route. One quarter of routes have current and obsolete HTTP latest rows in
every workspace. Half of the current rows are alive; obsolete rows deliberately
disagree with current health. It verifies the exact count and list result and
uses `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` to assert that latest-statistic and
ownership reads do not expand through the other owners.

The fixture uses a temporary schema in a disposable PostgreSQL database. Its
original foreign keys are validated after bulk loading and before measurement.

## Measurements

On local PostgreSQL 17, with 10,000 routes, eight workspaces, 80,000 ownership
rows, and 40,000 populated latest rows, the previous count took 18.7 ms and the
scoped count took 8.7 ms. The scoped plan read 5,000 latest rows and 10,000
ownership rows and returned 1,250 eligible routes. These are local comparison
timings, not a production latency guarantee.

The baseline query is retained by the regression for explicit comparison runs.
Set a disposable `MAGPIE_TEST_POSTGRES_DSN`, then run from `magpie-backend`:

```bash
MAGPIE_TEST_ROTATOR_ROUTES=1000000 \
MAGPIE_TEST_ROTATOR_BASELINE=1 \
MAGPIE_TEST_ROTATOR_EVIDENCE_DIR=/absolute/path/to/rotator-evidence \
  go test ./internal/database -run '^TestRotatorEvidence' -count=1 -v
```

The test writes before/after plans and latency JSON when the evidence directory
is set. It does not establish performance for long retained history with uptime
filters, other protocols, external proxy latency, or an entire deployment under
the representative soak.

## Checker operation budget

This changes rotator SQL only. It adds zero checker-loop database queries,
cryptographic operations, route hashes, JSON encodes, or Redis commands. Queue
payloads, credential defaults, and scheduling-only normal requeues are unchanged.
Rotator counting retains one database query per distinct filter combination in
the list request, and selected-route credential loading retains its existing
query and decryption behavior.
