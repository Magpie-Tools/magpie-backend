#!/usr/bin/env bash
# Writes the SQL-log digest of the tests in scripts/sql-log.tests to the file
# named by the first argument. See docs/onboarding/sql-log.md.
#
#   scripts/sql-log.sh /tmp/before.sqllog
#   git switch other-revision && scripts/sql-log.sh /tmp/after.sqllog
#   diff /tmp/before.sqllog /tmp/after.sqllog
#
# It starts its own PostgreSQL and Redis containers with unique names on free
# ports and removes them on exit. Existing containers are never touched.
set -euo pipefail

out="${1:?usage: scripts/sql-log.sh OUTPUT-FILE}"
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "sql-log: docker is required and its daemon must be reachable" >&2
  exit 1
fi

suffix="$$-$(date +%s)"
postgres="magpie-sqllog-postgres-${suffix}"
redis="magpie-sqllog-redis-${suffix}"
cleanup() { docker rm -f "$postgres" "$redis" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$postgres" \
  -e POSTGRES_USER=magpie_ci -e POSTGRES_PASSWORD=magpie_ci -e POSTGRES_DB=magpie_ci \
  -p 127.0.0.1::5432 "${POSTGRES_IMAGE:-postgres:17}" \
  -c log_statement=all -c "log_line_prefix=SQL|%p|%v|" -c log_parameter_max_length=0 \
  -c log_min_error_statement=panic -c max_connections=300 \
  -c logging_collector=on -c log_filename=sql.log -c log_rotation_age=0 -c log_rotation_size=0 >/dev/null
docker run -d --name "$redis" -p 127.0.0.1::6379 "${REDIS_IMAGE:-redis:7-alpine}" >/dev/null
postgres_port="$(docker port "$postgres" 5432/tcp | head -n1 | sed 's/.*://')"
redis_port="$(docker port "$redis" 6379/tcp | head -n1 | sed 's/.*://')"

deadline=$((SECONDS + ${STARTUP_TIMEOUT:-60}))
until docker exec -e PGPASSWORD=magpie_ci "$postgres" psql -h 127.0.0.1 -U magpie_ci -d magpie_ci -tAc 'select 1' >/dev/null 2>&1; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "sql-log: PostgreSQL was not ready in time" >&2
    exit 1
  fi
  sleep 1
done

export MAGPIE_TEST_POSTGRES_DSN="host=127.0.0.1 port=${postgres_port} user=magpie_ci password=magpie_ci dbname=magpie_ci sslmode=disable"
export MAGPIE_TEST_REDIS_URL="redis://127.0.0.1:${redis_port}"
export MAGPIE_TEST_QUEUE_REDIS_URL="redis://127.0.0.1:${redis_port}/13"

# Each line of sql-log.tests is "package pattern". Packages run one at a time so
# their statements do not interleave within a backend process.
while read -r package pattern; do
  case "$package" in ''|'#'*) continue ;; esac
  go test "$package" -run "$pattern" -count=1 -p 1 >/dev/null || {
    echo "sql-log: go test $package -run '$pattern' failed" >&2
    exit 1
  }
done < scripts/sql-log.tests

# The logging collector writes each message whole. The container's stderr can
# interleave long statements from concurrent backends.
docker exec "$postgres" sh -c 'cat "$PGDATA"/log/sql.log' | go run ./internal/tools/sqllog -ignore scripts/sql-log.ignore > "$out"
head -n 1 "$out"
