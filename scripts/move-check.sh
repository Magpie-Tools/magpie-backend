#!/usr/bin/env bash
# Verifies that the working tree is a pure move relative to a base revision:
# the rename-only diff of design D10. See internal/tools/movecheck.
#
#   scripts/move-check.sh HEAD~1
set -euo pipefail
base="${1:?usage: scripts/move-check.sh BASE-REVISION}"
cd "$(dirname "$0")/.."
old="$(mktemp -d)"
trap 'rm -rf "$old"' EXIT
git archive "$base" | tar -x -C "$old"
go run ./internal/tools/movecheck "$old" .
