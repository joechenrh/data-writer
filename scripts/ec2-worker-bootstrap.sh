#!/bin/bash
# scripts/ec2-worker-bootstrap.sh
#
# Runs on the c5.2xlarge worker instance at boot. Replaces (or wraps) the
# existing prebuilt-binary flow with: claim task, fetch generators_go,
# (if non-empty) rebuild mockingbird-worker with user code, run the shard.
#
# Prerequisites (baked into AMI — see docs/ec2-worker-ami.md):
#   - /opt/data-writer: git-clone of data-writer source
#   - /opt/data-writer/bin/mockingbird-worker: baseline prebuilt binary (no user code)
#   - Go toolchain on PATH (version matches go.mod)
#   - $GOCACHE pre-warmed via `go build ./cmd/mockingbird-worker` during AMI bake
#   - $DSN: pgx connection string (passed via cloud-init environment or user-data)
#   - $TASK_ID, $SHARD, $SHARD_TOTAL: passed via user-data (baked by launcher per-worker)
set -euo pipefail

cd /opt/data-writer

# 0. Sanity check inputs (launcher bakes these into user-data).
: "${DSN:?DSN env var is required}"
: "${TASK_ID:?TASK_ID env var is required}"
: "${SHARD:?SHARD env var is required}"
: "${SHARD_TOTAL:?SHARD_TOTAL env var is required}"

# 1. Dump generators_go to src/user/user_gens.go (empty = no user code).
./bin/mockingbird-worker dump-generators -dsn "$DSN" -task-id "$TASK_ID" > src/user/user_gens.go.new

if [[ -s src/user/user_gens.go.new ]]; then
  mv src/user/user_gens.go.new src/user/user_gens.go

  # 2. Generate init() registration + rebuild.
  if ! go run ./cmd/codegen -in ./src/user -out ./src/user/registry_gen.go 2> /tmp/build.err; then
    ./bin/mockingbird-worker report-failure -dsn "$DSN" -task-id "$TASK_ID" -err-file /tmp/build.err
    shutdown -h now
    exit 1
  fi
  if ! go build -o ./bin/mockingbird-worker ./cmd/mockingbird-worker 2> /tmp/build.err; then
    ./bin/mockingbird-worker report-failure -dsn "$DSN" -task-id "$TASK_ID" -err-file /tmp/build.err
    shutdown -h now
    exit 1
  fi
else
  # No user code — discard the empty file and keep the AMI's prebuilt binary.
  rm -f src/user/user_gens.go.new
fi

# 3. Run the shard with the binary (either prebuilt or freshly built).
./bin/mockingbird-worker run -dsn "$DSN" -task-id "$TASK_ID" \
  -shard "$SHARD" -shard-total "$SHARD_TOTAL"

# 4. Shut down so the instance auto-terminates (matches existing worker behavior).
shutdown -h now
