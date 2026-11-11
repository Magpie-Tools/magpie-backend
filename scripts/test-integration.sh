#!/usr/bin/env bash
# Runs the test suite against disposable PostgreSQL and Redis containers, the
# way CI does. The containers get unique names and free local ports, and are
# removed on exit. Existing containers are never touched.
#
#   scripts/test-integration.sh            go test and go test -race, all packages
#   RACE=0 scripts/test-integration.sh     skip the race run
#   scripts/test-integration.sh ./internal/database -run TestName
#
# Environment: POSTGRES_IMAGE (postgres:17), REDIS_IMAGE (redis:7-alpine),
# STARTUP_TIMEOUT seconds (60).
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "test-integration: docker is required to provision PostgreSQL and Redis" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "test-integration: the docker daemon is not reachable" >&2
  exit 1
fi

suffix="$$-$(date +%s)"
postgres="magpie-it-postgres-${suffix}"
redis="magpie-it-redis-${suffix}"
timeout="${STARTUP_TIMEOUT:-60}"
race_binary=""

cleanup() {
  docker rm -f "$postgres" "$redis" >/dev/null 2>&1 || true
  if [ -n "$race_binary" ]; then rm -f "$race_binary"; fi
}
trap cleanup EXIT

docker run -d --name "$postgres" \
  -e POSTGRES_USER=magpie_ci -e POSTGRES_PASSWORD=magpie_ci -e POSTGRES_DB=magpie_ci \
  -p 127.0.0.1::5432 "${POSTGRES_IMAGE:-postgres:17}" >/dev/null
docker run -d --name "$redis" -p 127.0.0.1::6379 "${REDIS_IMAGE:-redis:7-alpine}" >/dev/null

postgres_port="$(docker port "$postgres" 5432/tcp | head -n1 | sed 's/.*://')"
redis_port="$(docker port "$redis" 6379/tcp | head -n1 | sed 's/.*://')"

deadline=$((SECONDS + timeout))
# The image restarts PostgreSQL once after initialization, so wait for a query
# over TCP rather than for the first socket.
until docker exec -e PGPASSWORD=magpie_ci "$postgres" psql -h 127.0.0.1 -U magpie_ci -d magpie_ci -tAc 'select 1' >/dev/null 2>&1; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "test-integration: PostgreSQL was not ready within ${timeout}s" >&2
    docker logs "$postgres" 2>&1 | tail -n 20 >&2
    exit 1
  fi
  sleep 1
done
until [ "$(docker exec "$redis" redis-cli ping 2>/dev/null)" = "PONG" ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "test-integration: Redis was not ready within ${timeout}s" >&2
    exit 1
  fi
  sleep 1
done

export MAGPIE_TEST_POSTGRES_DSN="host=127.0.0.1 port=${postgres_port} user=magpie_ci password=magpie_ci dbname=magpie_ci sslmode=disable"
export MAGPIE_TEST_REDIS_URL="redis://127.0.0.1:${redis_port}"
export MAGPIE_TEST_QUEUE_REDIS_URL="redis://127.0.0.1:${redis_port}/13"
echo "test-integration: PostgreSQL on 127.0.0.1:${postgres_port}, Redis on 127.0.0.1:${redis_port}"

if [ "$#" -eq 0 ]; then
  set -- ./...
fi

go test "$@" -count=1
if [ "${RACE:-1}" != "0" ]; then
  race_binary="$(mktemp -t magpie-race.XXXXXX)"
  go build -race -o "$race_binary" ./cmd/magpie
  MAGPIE_TEST_BACKEND_BINARY="$race_binary" go test -race "$@" -count=1
fi
