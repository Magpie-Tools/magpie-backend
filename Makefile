# Required checks. See AGENTS.md.

.PHONY: test test-race vet build check test-integration

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

vet:
	go vet ./...

build:
	go build -o /dev/null ./cmd/magpie

check: test test-race vet build

# Starts disposable PostgreSQL and Redis containers on free ports, runs the
# suite against them like CI, and removes them. RACE=0 skips the race run.
test-integration:
	scripts/test-integration.sh
