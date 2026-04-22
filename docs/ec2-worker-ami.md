# EC2 Worker AMI — User Generator Support

The worker instance needs additional tools when users provide custom Go
generators for a task. This document describes what to bake into the AMI
and how the existing launcher must adapt.

## What changes

Previously, the worker AMI only needed `awscli` to download a prebuilt
`data-writer` binary from S3. With user-generator support, the worker may
need to recompile `data-writer` on boot with the user's `.go` file linked
in, so the AMI must include:

1. **Go toolchain** — version matches the project's `go.mod`.
   - Install `go1.24.x` (or whatever `go.mod` says).
   - Verify with `go version`.

2. **Data-writer source tree at `/opt/data-writer`** — a clean checkout of
   the main branch, matching the branch you want to ship. Clone and tag the
   state you bake in:
   ```bash
   git clone git@github.com:pingcap-qe/data-writer /opt/data-writer
   cd /opt/data-writer
   git checkout <commit>
   ```

3. **Warmed `$GOCACHE`** — so subsequent builds with a small user delta
   finish in seconds, not 30 seconds:
   ```bash
   cd /opt/data-writer
   go build ./src        # warms the cache
   go run ./cmd/codegen -in ./src/user -out ./src/user/registry_gen.go
   # Now `go build ./src` is ~1–2 s on top of that warm cache.
   ```

4. **Baseline binary at `/opt/data-writer/bin/data-writer`** — a prebuilt
   binary with NO user code linked in. Used for:
   - `-claim-task` / `-dump-generators` / `-report-failure` before any
     rebuild happens.
   - Running tasks that have no `generators_go` set (skips rebuild entirely).

   ```bash
   go build -o /opt/data-writer/bin/data-writer ./src
   ```

## Launcher changes

The existing launcher (t3.micro, `/tmp/ec2-launcher.sh`) pokes out per-worker
user-data that historically did `aws s3 cp ... data-writer && ./data-writer -worker ...`.

To pick up the new flow, change the per-worker user-data to invoke
`scripts/ec2-worker-bootstrap.sh` with `DSN`, `TASK_ID`, `SHARD`, `SHARD_TOTAL`
exported as environment variables. Something like:

```bash
#!/bin/bash
export DSN="postgres://..."
export TASK_ID=$TID
export SHARD=$SH
export SHARD_TOTAL=$ST

exec /opt/data-writer/scripts/ec2-worker-bootstrap.sh
```

(Replace the S3-download of `data-writer` from the old user-data template —
the AMI now carries both source and baseline binary.)

## Cost implications

- AMI size: +~2 GB for Go toolchain + source + GOCACHE. Still well under
  AL2023 EBS defaults.
- Worker cold start: +3–5 s for warm-cache rebuild when user code is
  present. Invisible compared to the c5.2xlarge boot time itself (~30 s).

## Fallback

If the AMI is reverted to the old image and a task has `generators_go`
set, the worker will fail early at step 1 (missing `bin/data-writer` with
the new `-dump-generators` subcommand) and the task will stall in
`launching`. The dispatch server (not this worker) should be conservative
and require `target=ec2` — which it already does for any task with
`generators_go`, so the only way to hit this failure is a mis-matched AMI.
