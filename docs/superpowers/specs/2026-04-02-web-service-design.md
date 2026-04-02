# data-writer Web Service Design

## Overview

Add an HTTP server mode to data-writer, turning it into a web service with a dashboard for triggering data generation tasks via browser. Only the `create` operation is exposed. Single-task execution (reject new submissions while running). Threads fixed at 16.

## Startup

```bash
./bin/data-writer -serve -port 8080
```

- `-serve` flag enters HTTP server mode. Without it, CLI behavior is unchanged.
- `-port` defaults to `8080`.

## API

### `GET /`

Serves the embedded static frontend (index.html, app.js, style.css) via `embed.FS`.

### `POST /api/create`

Submits a data generation task. Returns `409 Conflict` if a task is already running.

Request body (JSON):

```json
{
  "sql": "CREATE TABLE t1 (...)",
  "path": "s3://bucket/prefix",
  "prefix": "test.t1",
  "start_fileno": 0,
  "end_fileno": 100,
  "rows": 60000,
  "format": "csv",
  "s3": {
    "region": "us-east-1",
    "access_key": "...",
    "secret_key": "...",
    "provider": "aws",
    "endpoint": "..."
  },
  "gcs": {
    "credential": "/path/to/cred.json"
  }
}
```

- `s3`/`gcs` fields are optional; include only when `path` starts with `s3://` or `gcs://`.
- Threads fixed at 16 (not configurable via API).
- Response: `200 OK` with `{"status": "started"}` or `409` with `{"error": "task already running"}`.

### `GET /api/status`

Returns current task state. Reuses the existing progress renderer data.

```json
{
  "state": "running",
  "progress": "45%",
  "speed": "368.5MiB/s",
  "files_written": 12,
  "total_files": 100,
  "written_size": "11.25GiB",
  "error": ""
}
```

`state` values: `idle`, `running`, `completed`, `failed`.

When `state` is `idle`, all other fields are zero/empty.

## Frontend

Single HTML page with two sections:

### Form Section
- SQL schema: multiline textarea
- Storage path: text input (local path, `s3://...`, or `gcs://...`)
- Prefix: text input (default `test.t1`)
- File range: start_fileno + end_fileno inputs
- Rows per file: number input (default `60000`)
- Format: dropdown (csv / parquet)
- S3 credentials: region, access_key, secret_key, provider, endpoint (shown only when path starts with `s3://`)
- GCS credentials: credential path (shown only when path starts with `gcs://`)
- Submit button (disabled while task is running)

### Status Section
- Progress bar
- Speed, files written, total size
- Error message (if failed)
- Polls `GET /api/status` every 2 seconds while a task is running

## File Changes

```
src/
  main.go          # Add -serve/-port flags, branch to server mode
  server.go        # NEW: HTTP server, handlers, embed directive
  operations.go    # Unchanged
public/            # NEW directory
  index.html       # Dashboard page
  app.js           # Form logic, status polling
  style.css        # Styling
```

`public/` is embedded into the binary via `//go:embed public/*`.

## Deployment

1. `make build` produces a single binary with embedded frontend.
2. Run `./bin/data-writer -serve -port 8080` in a terminal (like raas).
3. Add ingress rule to `~/.cloudflared/config.yml`:
   ```yaml
   - hostname: datagen.ingresses.org
     service: http://localhost:8080
   ```
4. Restart cloudflared tunnel.

## Explicitly Out of Scope

- No database: single-task state lives in memory.
- No authentication: consistent with raas/import.
- No task history: only current/last task visible.
- No CLI behavior changes: existing flags and operations untouched.
- Threads not user-configurable: hardcoded to 16.
