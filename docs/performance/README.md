# Performance notes

Each note records measurements taken on one revision, host and date. A note
marked **historical** describes the code as it was then: its commands, file
names and numbers are not kept up to date, and `internal/archtest` does not
check its links or `go test` snippets. Historical measurements cannot establish
that a present change is performance neutral; every relevant change needs fresh
evidence.

A note marked **current** is maintained. Its relative links must resolve and
its `go test` snippets must name tests that exist.

Every file in this directory has a row here. Add a row when adding a note.

| Note | Status | Subject |
| --- | --- | --- |
| [backend-longevity-fixes-2026-10-13.md](backend-longevity-fixes-2026-10-13.md) | historical | Backend longevity fixes, 13 October 2026 |
| [backend-scale-fixes-2026-10-04.md](backend-scale-fixes-2026-10-04.md) | historical | Backend scale fixes |
| [backend-scale-lease-followup-2026-10-04.md](backend-scale-lease-followup-2026-10-04.md) | historical | Lease and statistics shutdown follow-up |
| [backend-scale-lease-followup-verification-2026-10-04.md](backend-scale-lease-followup-verification-2026-10-04.md) | historical | Lease follow-up verification |
| [backend-scale-legacy-rekey-fix-2026-10-04.md](backend-scale-legacy-rekey-fix-2026-10-04.md) | historical | Legacy hash migration lease fix |
| [backend-scale-legacy-rekey-verification-2026-10-04.md](backend-scale-legacy-rekey-verification-2026-10-04.md) | historical | Independent verification of legacy rekey migration |
| [backend-scale-review-2026-10-04.md](backend-scale-review-2026-10-04.md) | historical | Backend scaling review |
| [backend-scale-verification-2026-10-04.md](backend-scale-verification-2026-10-04.md) | historical | Backend scale fix verification |
| [checker-evidence-pruning-2026-10-21.md](checker-evidence-pruning-2026-10-21.md) | historical | Bound obsolete latest-evidence pruning |
| [checker-noop-writes-2026-10-21.md](checker-noop-writes-2026-10-21.md) | historical | Skip unchanged failure counters and proxy metadata |
| [checker-owner-fanout-2026-10-31.md](checker-owner-fanout-2026-10-31.md) | historical | Share judge picks and stop rewriting dead-route streaks |
| [dashboard-boundary-history-2026-10-22.md](dashboard-boundary-history-2026-10-22.md) | historical | Restrict dashboard history to contributing boundary rows |
| [dashboard-check-counts-2026-10-27.md](dashboard-check-counts-2026-10-27.md) | historical | Dashboard check counts through one ownership join |
| [dashboard-current-evidence-2026-10-21.md](dashboard-current-evidence-2026-10-21.md) | historical | Dashboard evidence reads and cancellation |
| [dashboard-demand-cache-2026-10-24.md](dashboard-demand-cache-2026-10-24.md) | historical | Dashboard cache demand and freshness |
| [dashboard-inventory-summary-2026-10-27.md](dashboard-inventory-summary-2026-10-27.md) | historical | One inventory read for cold dashboard summaries |
| [dashboard-startup-2026-10-22.md](dashboard-startup-2026-10-22.md) | historical | Dashboard ownership joins on the retained million-route fixture |
| [dashboard-startup-lifecycle-2026-10-22.md](dashboard-startup-lifecycle-2026-10-22.md) | historical | Dashboard prewarm startup bound, 22 October 2026 |
| [failure-actions.md](failure-actions.md) | current | Proxy failure-action operation budget |
| [go-1.27.1.md](go-1.27.1.md) | historical | Go 1.27.1 and dependency upgrade |
| [go-1.27.2.md](go-1.27.2.md) | historical | Go 1.27.2 security update |
| [go-1.27.3.md](go-1.27.3.md) | historical | Go 1.27.3 security patch and component measurements |
| [production-readiness-fixes-2026-10-14.md](production-readiness-fixes-2026-10-14.md) | historical | Production readiness fixes, 14 October 2026 |
| [projection-deadline-subprefix-candidate-2026-10-25.md](projection-deadline-subprefix-candidate-2026-10-25.md) | historical | Projection deadline subprefix candidate, 25 October 2026 |
| [projection-index-updates-2026-10-21.md](projection-index-updates-2026-10-21.md) | historical | Remove duplicate proxy filter indexes |
| [projection-noop-filter-2026-10-28.md](projection-noop-filter-2026-10-28.md) | historical | Skip unchanged projection rows before conflict locking |
| [projection-ordered-rounds-2026-10-26.md](projection-ordered-rounds-2026-10-26.md) | historical | Ordered rounds for pending projection work |
| [projection-route-locality-2026-10-25.md](projection-route-locality-2026-10-25.md) | historical | Shared-route projection locality |
| [projection-slow-success-2026-10-28.md](projection-slow-success-2026-10-28.md) | historical | Projection width after slow successful commits |
| [projection-window-2026-10-27.md](projection-window-2026-10-27.md) | historical | Projection width after a child deadline |
| [proxy-page-filter-reads-2026-10-21.md](proxy-page-filter-reads-2026-10-21.md) | historical | Proxy page and filter-option reads at shared-inventory scale |
| [proxy-reimport-inventory-2026-10-26.md](proxy-reimport-inventory-2026-10-26.md) | historical | Reimport inventory work |
| [proxy-snapshot-ownership-2026-10-26.md](proxy-snapshot-ownership-2026-10-26.md) | historical | Scoped ownership for alive-count snapshots |
| [queue-interval-startup.md](queue-interval-startup.md) | historical | Queue interval initialization |
| [read-model-pair-budget-2026-10-22.md](read-model-pair-budget-2026-10-22.md) | historical | Projection batches at shared-workspace scale |
| [read-model-projection-2026-10-21.md](read-model-projection-2026-10-21.md) | historical | Bound workspace proxy projection to its candidate evidence |
| [rotator-current-evidence-2026-10-21.md](rotator-current-evidence-2026-10-21.md) | historical | Rotator current-evidence query scope |
| [snapshot-alive-count-2026-10-21.md](snapshot-alive-count-2026-10-21.md) | historical | Count current alive routes before joining ownership |
| [startup-inventory-2026-10-28.md](startup-inventory-2026-10-28.md) | historical | Select each startup route without expanding shared owners |
| [statistics-consumer-prefixes-2026-10-22.md](statistics-consumer-prefixes-2026-10-22.md) | historical | Bound statistics consumer transactions by evidence fanout |
| [statistics-transaction-order-2026-10-22.md](statistics-transaction-order-2026-10-22.md) | historical | Statistics transaction order |
| [tag-checker-settings.md](tag-checker-settings.md) | current | Tag checker settings operation budget |
| [workspace-list-counts-2026-10-21.md](workspace-list-counts-2026-10-21.md) | historical | Workspace list count batching, 21 October 2026 |
| [hot-path-operation-baseline.md](hot-path-operation-baseline.md) | current | Per-check operation counts of the checker loop |
