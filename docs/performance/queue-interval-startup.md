# Queue interval initialization

The reported route had repeated successful SOCKS5 results about 1.4–1.8 seconds
apart, while the loaded global checker timer was 65 minutes at the start of the
investigation. Tag profiles do not define a check interval. Existing queue members
keep their due times, so a corrupted shared interval first affects routes that
are already due; other routes retain their future schedules.

Queue package initialization started interval listeners before settings loaded.
Those listeners published the one-second initial value to shared Redis. A test
process does not load application settings, so merely importing the queue could
replace a running backend's interval. Startup and later updates could also lose
the newest duration when a listener's one-slot channel was already full.

## Reproduction and repair

On disposable Redis 7, set `magpie:queue:proxy:interval_ms` to `3900000`, then run
the unmodified backend's `TestBulkRequeuePreservesActiveLease`. The test uses its
own queue fixture, but the package initializer changes the disposable shared
interval to `1000`. A subprocess regression reproduces the same overwrite for
both proxy and scrape intervals. Two notification regressions retain the
one-second value when an hour-scale update arrives before the initial value is
read. All three regressions fail before the repair.

Application bootstrap now starts both interval listeners after loading file and
Redis settings. Tests and migration-only processes do not start those listeners.
Listener registration and updates share a lock, and each notification channel
retains the newest pending duration. Listeners stop when application context is
canceled. Startup restores stale interval state; ordinary requeues apply that
duration without rescheduling every existing route.

The startup regression begins with one-second Redis state, loads a 65-minute
checker timer and two-hour scrape timer, and verifies both shared values. It
dequeues and completes a proxy, checks that its next due time is at least 65
minutes later, verifies the payload is unchanged, and confirms it cannot be
dequeued immediately again.

## Per-check operation budget

| Category | Added operations per proxy check |
| --- | ---: |
| Cryptography, including route fingerprints | 0 |
| JSON encoding | 0 |
| Redis commands | 0 |
| Database queries, including credential loads | 0 |
| Queue payload writes on normal requeue | 0 |

The warm default cycle still uses dequeue `EVALSHA`, interval `GET`, and
completion `EVALSHA`. Payload decode, stored hash reuse, encryption defaults,
legacy readers, and scheduling scripts are unchanged. The repair changes only
startup synchronization and notification delivery and adds no work to the
proxy-check loop.

## Validation

All required backend checks passed:

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `go build ./cmd/magpie`

The test and race runs used disposable PostgreSQL 17 and Redis 7, with separate
Redis databases for queue and statistics integration fixtures. Existing tests
for cipher-free stored-hash reuse and payload preservation remain passing.
Repeating the original Redis reproduction after the fix leaves the interval at
`3900000`. The documentation production build also passed.

The running development backend still uses its previously built executable and
needs a restart to load the code repair. Its live configuration changed from
65 minutes to 605 minutes during the investigation; the shared Redis interval
matched the latter value at the final read. No global settings were saved by
this investigation.
