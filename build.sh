#!/usr/bin/env bash
set -euo pipefail
mkdir -p bin
go build -o bin/mockingbird        ./cmd/mockingbird
go build -o bin/mockingbird-worker ./cmd/mockingbird-worker
go build -o bin/mockingbird-cli    ./cmd/mockingbird-cli
go build -o bin/codegen            ./cmd/codegen
