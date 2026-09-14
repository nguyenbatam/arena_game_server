#!/bin/sh
set -e
cd "$(dirname "$0")/.."
# proto/ is the shared FE+BE contract and stays at the repo root; the generated
# Go lands inside the backend module.
protoc -I proto --go_out=backend --go_opt=module=github.com/nguyenbatam/arena_game_server proto/arena/v1/arena.proto
echo "generated backend/gen/pb/arena.pb.go"
