---
status: accepted
---

# Serve the previous dashboard health while checker settings apply

[ADR 0017](0017-serve-dashboards-from-shared-generation-stamped-snapshots.md)
masked the dashboard's health parts while `checker_dirty` was true and hid
them after a newer checker generation until a build computed them again.
These parts are the alive count, the judge breakdown, recent checks and the
fastest alive routes. A build that ran while the workspace was dirty also
published a payload without health, which erased the last computed health.
Applying new checker settings to a large workspace takes minutes. For that
whole time the dashboard's main figures were blank, and the page stopped
asking for updates once its 60 s window ran out.

## Decision

- **Storage:** the health parts move out of the payload into their own
  `health` column. `checker_generation` names the generation they were built
  under, and `health_collected_at` names the claim time of the build that
  computed them.
- **Publishing:** a build that computes current health replaces all three. A
  build that cannot keeps them and replaces only the inventory payload. That
  includes a build on a dirty workspace and a build whose generation changed
  while it ran. The payload version becomes 2, and version 1 rows expire
  under the normal seven-day retention.
- **Serving:** health parts that are not current are served as the previous
  result instead of being hidden. The view carries:
  - `healthOutdated`;
  - `healthCollectedAt`;
  - `checkerSettingsPending`, which replaces `healthMasked` and means the
    settings are still being applied.

  Health parts are null only when no build has ever computed them.
- **Unchanged from ADR 0017:**
  - the status table;
  - the claim rules;
  - a dirty workspace alone still never makes a view `PENDING` or triggers a
    rebuild;
  - a newer generation still makes the view `PENDING` and claims a build
    once the cooldown allows.
- **Frontend:**
  - It shows the previous results immediately, labelled "Previous results"
    with their age and the reason.
  - "Wait for new results" hides them until results under the new settings
    arrive. The choice is remembered per browser.
  - While the results are outdated, the page keeps asking for updates: every
    2–4 s for the first minute, then every 10–15 s, for at most 30 minutes.
- **Alive history:** the series in `viewer.proxySnapshots` gets the
  snapshot's alive count stamped with `healthCollectedAt`, so an outdated
  count lands at its own time.

## Consequences

- During a settings change the dashboard shows figures counted under the
  earlier settings. They are always marked outdated, never presented as
  current. A viewer who prefers blanks chooses to wait.
- Each row stores at most one previous health result. The 256 KiB cap covers
  the payload and the health together.
- Retained health can be older than the row's last build. `healthCollectedAt`
  says how old it is.
- The `healthMasked` field is removed without a deprecation period. No
  release has shipped `dashboardView` yet.
