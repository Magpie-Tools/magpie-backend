# Magpie Backend

The Go backend for [Magpie](https://magpie.tools), a self-hosted proxy manager.
It provides the REST and GraphQL APIs, background jobs, proxy checking,
scraping, reputation calculation, and rotating proxy listeners.

## Requirements

- Go `1.27.2` or newer
- PostgreSQL `17`
- Redis `7`

The complete local stack is maintained by the main Magpie distribution
repository. For backend-only development, start PostgreSQL and Redis, configure
the required environment variables, and run:

```bash
go run ./cmd/magpie
```

The API listens on `http://localhost:5656` by default.

## Development test data

Stop all backends connected to the development database, then preview or reset
three test accounts:

```bash
go run ./cmd/seed-dev-data --dry-run
go run ./cmd/seed-dev-data --reset
DEVELOPMENT_FIXTURES=true go run ./cmd/magpie
```

The default accounts are `username@gmx.at`, `username2@gmx.at`, and
`username3@gmx.at`. Existing IDs, password hashes, and administrator roles are
preserved. For a fresh database, migrate first and set `MAGPIE_DEV_PASSWORD` to
the password for the new accounts. Use `--accounts` to supply three other email
addresses. A workspace shared with any unselected user blocks the reset.

The default dataset has five workspaces, 5,000 routes, 30 days of check history,
tags and checker rules, source health, rotating proxy configurations, alerts,
incidents, usage, and pending invitations. Set `--proxies-per-workspace`,
`--history-days`, and `--seed` to change its size or deterministic values.
Rerun `--reset` to restore the dataset with dates relative to today. Each reset
uses one PostgreSQL transaction and reconciles only affected Redis queue
entries. It never flushes Redis. Back up data you want to keep before resetting.

Fixtures use documentation IP ranges and `.invalid` URLs. Alert destinations
are disabled. `DEVELOPMENT_FIXTURES=true` keeps the API and dashboard cache
running while skipping background jobs and rotating listeners, so synthetic
results survive restarts. Set it in the backend's local `.env` to keep using
fixtures. API-created rotators are saved without opening TCP or UDP listeners.
Restart with fixture mode disabled to activate them. Production mode rejects
both seeding and fixture startup.

See the [local development guide](https://magpie.tools/docs/getting-started/local-development)
for the reset workflow and account permissions.

## Database migrations

Run migrations as a one-off command before starting a new production backend:

```bash
go run ./cmd/magpie --migrate-only
```

`DB_AUTO_MIGRATE=false` now prevents every startup schema change. The migration
command overrides that setting, applies the schema and data backfills, then
exits.

Canonical proxy hosts, including provider gateway hostnames, remain visible to
a database reader. Literal IP hosts also use a nullable PostgreSQL `inet`
projection for indexed subnet operations. Proxy usernames and passwords are
encrypted on each user's proxy access row. Redis queue payloads keep hosts and
credentials in plaintext by default because proxy checking is a high-volume hot
path. Protect Redis as trusted infrastructure. Set
`PROXY_QUEUE_ENCRYPT_CREDENTIALS=true` only when the added per-check decryption
cost is acceptable.

## Validation

Proxy export regression tests cover batching, request cancellation, response
deadlines, and interrupted transfers. To also exercise the frontend nginx
template on Linux with Docker and the frontend repository cloned alongside this
one, run:

```bash
MAGPIE_EXPORT_NGINX_IMAGE=nginx:alpine go test ./internal/app/server -run TestProxyExportThroughNginx -count=1 -v
```

This starts an isolated nginx container and a test backend, waits 31 seconds
before sending an export batch, and checks timeout errors and client
disconnects. It removes the test container afterward.

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./cmd/magpie
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
```

Set `MAGPIE_TEST_POSTGRES_DSN` to an isolated PostgreSQL test database to run
the storage and migration integration checks. CI supplies that database.
The [Go 1.27.1 upgrade report](docs/performance/go-1.27.1.md) records validation
and queue/checker benchmark results. The [Go 1.27.2 security update](docs/performance/go-1.27.2.md)
records the follow-up fixes and validation.

## Container image

Build the backend image from this repository root:

```bash
docker build -t magpie-backend:dev .
```

After authenticating to the target registry, publish the default multi-platform
image with:

```bash
./scripts/push-docker-image.sh <tag>
```

Set `MAGPIE_BACKEND_IMAGE` to publish under another image name,
`DOCKER_PLATFORMS` to change target platforms, or `PUSH_LATEST=0` to avoid
updating the `latest` tag.

## Related repositories

- [Distribution and deployment](https://github.com/Magpie-Tools/magpie)
- [Frontend](https://github.com/Magpie-Tools/magpie-frontend)
- [Website](https://github.com/Magpie-Tools/magpie-website)
- [Documentation](https://github.com/Magpie-Tools/magpie-docs)

## License

Magpie is distributed under the GNU Affero General Public License v3.0.
