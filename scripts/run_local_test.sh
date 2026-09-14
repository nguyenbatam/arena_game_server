#!/usr/bin/env bash
# Run the same checks as CI, then bring up docker (Redis + Kafka + server + FE).
#
#   sh scripts/run_local_test.sh
#   sh scripts/run_local_test.sh --skip-race
#   sh scripts/run_local_test.sh --down
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SKIP_CI=0
SKIP_RACE=0
SKIP_PROTO=0
SKIP_LINT=0
SKIP_DOCKER=0
NO_BUILD=0
DOWN=0
FOLLOW=0

usage() {
	cat <<'EOF'
Usage: sh scripts/run_local_test.sh [options]

  (default)   CI checks, then docker compose up (server serves web/ at :8080)

  --skip-ci       skip gofmt / vet / lint / proto / test / race
  --skip-race     skip go test -race (faster)
  --skip-lint     skip golangci-lint
  --skip-proto    skip proto drift check
  --skip-docker   CI checks only, do not start the stack
  --no-build      docker compose up without --build
  --follow        attach to compose logs after the stack is up
  --down          tear the stack down and exit
  -h, --help
EOF
}

while [ $# -gt 0 ]; do
	case "$1" in
		--skip-ci) SKIP_CI=1 ;;
		--skip-race) SKIP_RACE=1 ;;
		--skip-lint) SKIP_LINT=1 ;;
		--skip-proto) SKIP_PROTO=1 ;;
		--skip-docker) SKIP_DOCKER=1 ;;
		--no-build) NO_BUILD=1 ;;
		--follow) FOLLOW=1 ;;
		--down) DOWN=1 ;;
		-h|--help) usage; exit 0 ;;
		*) usage >&2; echo "unknown flag: $1" >&2; exit 2 ;;
	esac
	shift
done

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() {
	command -v "$1" >/dev/null 2>&1 || die "missing $1"
}

compose() {
	if docker compose version >/dev/null 2>&1; then
		docker compose -f docker-compose.yml -f docker-compose.local.yml "$@"
	elif command -v docker-compose >/dev/null 2>&1; then
		docker-compose -f docker-compose.yml -f docker-compose.local.yml "$@"
	else
		die "docker compose is required"
	fi
}

wait_http() {
	local url="$1" label="$2" tries="${3:-60}"
	local i
	for i in $(seq 1 "$tries"); do
		if curl -fsS -o /dev/null "$url" 2>/dev/null; then
			log "$label ok  $url"
			return 0
		fi
		sleep 1
	done
	die "$label did not become ready: $url"
}

run_ci() {
	need go
	log "gofmt"
	(
		cd backend
		out="$(gofmt -l .)"
		if [ -n "$out" ]; then
			echo "these files are not gofmt'd:"
			echo "$out"
			exit 1
		fi
	)

	log "vet"
	(cd backend && go vet ./...)

	if [ "$SKIP_LINT" -eq 0 ]; then
		lint_bin=""
		gopath_lint="$(go env GOPATH)/bin/golangci-lint"
		if [ -x "$gopath_lint" ]; then
			lint_bin="$gopath_lint"
		elif command -v golangci-lint >/dev/null 2>&1; then
			lint_bin="$(command -v golangci-lint)"
		fi
		if [ -n "$lint_bin" ]; then
			log "golangci-lint"
			(cd backend && "$lint_bin" run ./...)
		else
			log "skip lint (golangci-lint not installed)"
		fi
	else
		log "skip lint"
	fi

	if [ "$SKIP_PROTO" -eq 0 ]; then
		if command -v protoc >/dev/null 2>&1; then
			log "proto gen + drift"
			if ! command -v protoc-gen-go >/dev/null 2>&1; then
				go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
				export PATH="$(go env GOPATH)/bin:$PATH"
			fi
			sh scripts/gen-proto.sh
			git diff --exit-code backend/gen/pb
		else
			log "skip proto drift (protoc not installed)"
		fi
	fi

	log "test"
	(cd backend && go test -count=1 -shuffle=on ./...)

	if [ "$SKIP_RACE" -eq 0 ]; then
		log "race"
		(cd backend && go test -race -count=1 -shuffle=on ./...)
	else
		log "skip race"
	fi
}

up_stack() {
	need docker
	need curl
	docker info >/dev/null 2>&1 || die "docker daemon is not running"

	if [ ! -f backend.env ]; then
		cp backend.env.example backend.env
		log "created backend.env from backend.env.example"
	fi

	local up_args=(up -d)
	if [ "$NO_BUILD" -eq 0 ]; then
		up_args+=(--build)
	fi
	log "docker compose ${up_args[*]}"
	compose "${up_args[@]}"

	wait_http "http://localhost:8080/healthz" "healthz" 90
	wait_http "http://localhost:8080/readyz" "readyz" 30
	wait_http "http://localhost:8080/" "arena FE" 10
	wait_http "http://localhost:8080/turn.html" "turn FE" 10
	wait_http "http://localhost:8080/platform.html" "platform FE" 10
	# The platform tier is on in the compose stack (PLATFORM_DSN points at the
	# postgres service), so its catalog answering is what says the pool came up
	# and the schema applied. A 404 here means the tier is off, not broken.
	wait_http "http://localhost:8080/platform/catalog" "platform API" 20

	cat <<'EOF'

stack is up — FE is served by the server container (web/ copied into the image)

  arena       http://localhost:8080
  turn-based  http://localhost:8080/turn.html
  platform    http://localhost:8080/platform.html   (register, then play — the match lands in your history)
  metrics     http://localhost:8080/metrics
  prometheus  http://localhost:9091
  ws          ws://localhost:8080/ws
  tcp         localhost:8081
  udp         localhost:8082

  one browser is enough: MIN_PLAYERS=1, bots fill the rest
  tear down:  sh scripts/run_local_test.sh --down

EOF

	if [ "$FOLLOW" -eq 1 ]; then
		compose logs -f
	fi
}

if [ "$DOWN" -eq 1 ]; then
	need docker
	log "docker compose down"
	compose down
	exit 0
fi

if [ "$SKIP_CI" -eq 0 ]; then
	run_ci
else
	log "skip CI checks"
fi

if [ "$SKIP_DOCKER" -eq 0 ]; then
	up_stack
else
	log "skip docker"
fi
