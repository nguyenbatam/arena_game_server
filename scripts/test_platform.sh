#!/usr/bin/env bash
# The Postgres half of the platform conformance suite.
#
#   sh scripts/test_platform.sh
#
# internal/platform runs one set of assertions against both of its Store
# implementations. The memory half runs with `make test`; the Postgres half
# skips itself unless PLATFORM_TEST_DSN is set, because CI cannot be made to
# depend on a database being installed. This starts a throwaway one, runs the
# suite against both, and takes it away again.
#
# The container is disposable on purpose — no volume, dropped at the end. The
# suite truncates between cases, so anything left behind would only ever be a
# way for one run to contaminate the next.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NAME="${PG_CONTAINER:-arena-platform-test}"
PORT="${PG_PORT:-55432}"
IMAGE="${PG_IMAGE:-postgres:18.2-alpine}"
DSN="postgres://arena:arena@localhost:${PORT}/arena?sslmode=disable"

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker is required"
docker info >/dev/null 2>&1 || die "docker daemon is not running"

cleanup() {
	log "removing $NAME"
	docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$NAME" >/dev/null 2>&1 || true
log "starting $IMAGE on :$PORT"
docker run -d --name "$NAME" \
	-e POSTGRES_USER=arena -e POSTGRES_PASSWORD=arena -e POSTGRES_DB=arena \
	-p "${PORT}:5432" "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
	if docker exec "$NAME" pg_isready -U arena -d arena >/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 1
done
[ "${ready:-0}" = 1 ] || die "postgres did not become ready"
log "postgres ready"

log "go test ./internal/platform (memory + postgres)"
(cd backend && PLATFORM_TEST_DSN="$DSN" go test -race -count=1 -shuffle=on -v ./internal/platform/)

# The app tests run on the memory store by default, which is right: CI cannot
# depend on a database. TestTheGatewayComesUpAgainstARealDatabase is the one
# that needs the DSN — the Postgres branch of startPlatform, the pool, the
# schema migration and the rating-store swap are only exercised through it.
log "go test ./internal/app (platform tier over HTTP, plus the real-database gateway)"
(cd backend && PLATFORM_TEST_DSN="$DSN" go test -race -count=1 -shuffle=on ./internal/app/)
