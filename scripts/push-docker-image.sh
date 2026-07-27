#!/usr/bin/env bash
set -euo pipefail

IMAGE="${MAGPIE_BACKEND_IMAGE:-kuuchen/magpie-backend}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required but was not found in PATH." >&2
  exit 1
fi

if ! docker buildx version >/dev/null 2>&1; then
  echo "Docker Buildx is required but is not available." >&2
  exit 1
fi

tag="${1:-}"
if [[ -z "${tag}" ]]; then
  tag="$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
fi

build_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
push_latest="${PUSH_LATEST:-1}"
platforms="${DOCKER_PLATFORMS:-linux/amd64,linux/arm64}"
builder="${BUILDX_BUILDER:-magpie-backend-multiarch}"

if ! docker buildx inspect "${builder}" >/dev/null 2>&1; then
  docker buildx create --name "${builder}" --driver docker-container --use >/dev/null
else
  docker buildx use "${builder}" >/dev/null
fi
docker buildx inspect "${builder}" --bootstrap >/dev/null

tags=(-t "${IMAGE}:${tag}")
if [[ "${push_latest}" == "1" ]]; then
  tags+=(-t "${IMAGE}:latest")
fi

echo "Building and pushing ${IMAGE}:${tag} for ${platforms}..."
docker buildx build \
  --file "${REPO_ROOT}/Dockerfile" \
  --builder "${builder}" \
  --platform "${platforms}" \
  --build-arg BUILD_VERSION="${tag}" \
  --build-arg BUILD_TIME="${build_time}" \
  "${tags[@]}" \
  --push \
  "${REPO_ROOT}"

echo "Backend image publish complete."
