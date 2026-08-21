# Magpie Backend

The Go backend for [Magpie](https://magpie.tools), a self-hosted proxy manager.
It provides the REST and GraphQL APIs, background jobs, proxy checking,
scraping, reputation calculation, and rotating proxy listeners.

## Requirements

- Go `1.26.x`
- PostgreSQL `17`
- Redis `7`

The complete local stack is maintained by the main Magpie distribution
repository. For backend-only development, start PostgreSQL and Redis, configure
the required environment variables, and run:

```bash
go run ./cmd/magpie
```

The API listens on `http://localhost:5656` by default.

## Database migrations

Run migrations as a one-off command before starting a new production backend:

```bash
go run ./cmd/magpie --migrate-only
```

`DB_AUTO_MIGRATE=false` now prevents every startup schema change. The migration
command overrides that setting, applies the schema and data backfills, then
exits.

Proxy IP addresses use PostgreSQL's native `inet` type and remain visible to a
database reader. Proxy usernames and passwords are encrypted on each user's
proxy access row. Redis queue payloads keep credentials in plaintext by default
because proxy checking is a high-volume hot path. Protect Redis as trusted
infrastructure. Set `PROXY_QUEUE_ENCRYPT_CREDENTIALS=true` only when the added
per-check decryption cost is acceptable.

## Validation

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/magpie
```

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
