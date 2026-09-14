.PHONY: env load-docker load-docker-down run run-platform test test-platform race fuzz replay load load-server load-10k load-turn load-turn-10k docker proto tidy k8s fmt lint local-test

run:
	cd backend && go run ./cmd/server

# The realtime tier plus the platform tier, with no database: accounts, wallets,
# inventory, history and the ladder all work and none of them survive a restart.
# Register at http://localhost:8080/platform.html.
run-platform:
	cd backend && PLATFORM_DSN=memory JWT_SECRET=dev-secret-not-for-deployment go run ./cmd/server

# -shuffle=on: a test that leans on package state — a Prometheus collector, a
# global — passes alone and fails only when something else ran first. Cheap
# insurance against the next one.
test:
	cd backend && go test -count=1 -shuffle=on ./...

# internal/platform runs one suite against both of its Store implementations.
# `make test` covers the memory half; the Postgres half skips itself without a
# database, so this starts a throwaway one and runs both.
test-platform:
	sh scripts/test_platform.sh

race:
	cd backend && go test -race -count=1 -shuffle=on ./...

# The two decoders that read bytes straight off the internet. CI runs their seed
# corpora with the normal test pass; this is the soak.
fuzz:
	cd backend && go test ./internal/protocol -run FuzzUnmarshalEnv -fuzz FuzzUnmarshalEnv -fuzztime 60s
	cd backend && go test ./internal/room -run FuzzApplyDelta -fuzz FuzzApplyDelta -fuzztime 60s

# Re-simulate recorded matches and check this build still reproduces them.
# Record some first: REPLAY_DIR=../replays make run
replay:
	cd backend && go run ./cmd/replay ../replays

# Every bot dials from one client IP, so the gateway's per-IP limiters would cap
# the whole fleet at HELLO_RATE_LIMIT connections. load-server is `run` with
# those limits off — local only; production keeps its defaults.
env:
	@test -f backend.env || (cp backend.env.example backend.env && echo "created backend.env from backend.env.example")
	@test -f backend.env && echo "backend.env ready"

# Brings the stack up with the load-test overrides layered on, then point the
# bots at it with load-10k / load-turn-10k.
load-docker: env
	docker compose -f docker-compose.yml -f docker-compose.loadtest.yml up -d --build

load-docker-down:
	docker compose -f docker-compose.yml -f docker-compose.loadtest.yml down

load-server:
	cd backend && HELLO_RATE_LIMIT=0 QUEUE_RATE_LIMIT=0 TURN_RATE_LIMIT=0 LOGIN_RATE_LIMIT=0 \
		MAX_CCU=30000 MIN_PLAYERS=1 QUEUE_TIMEOUT=1s go run ./cmd/server

load:
	cd backend && go run ./cmd/loadtest -n 1000 -ramp 5s -dur 20s

load-10k:
	cd backend && go run ./cmd/loadtest -n 10000 -ramp 25s -dur 30s

load-turn:
	cd backend && go run ./cmd/loadtest -mode turn -n 2000 -ramp 10s -dur 20s

load-turn-10k:
	cd backend && go run ./cmd/loadtest -mode turn -n 10000 -ramp 25s -dur 30s

proto:
	sh scripts/gen-proto.sh

fmt:
	cd backend && gofmt -l -w .

# Needs golangci-lint v2.13+ (built with Go 1.27). GOPATH/bin first so an older Homebrew binary is not used.
lint:
	cd backend && PATH="$(shell go env GOPATH)/bin:$$PATH" golangci-lint run ./...

tidy:
	cd backend && go mod tidy

# CI checks, then Redis + Kafka + server (serves web/ at :8080).
local-test:
	sh scripts/run_local_test.sh

docker:
	docker compose up --build

k8s:
	kubectl apply -k deploy/k8s/overlays/prod
