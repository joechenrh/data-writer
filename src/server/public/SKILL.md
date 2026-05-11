---
name: datagen
description: Generate CSV/Parquet/SQL test data via the data-writer (Mockingbird) API. Use when the user asks to generate data files, create test datasets, or produce synthetic data for S3/local storage, including custom per-column Go generators.
---

You have access to a data generation service at `https://datagen.ingresses.org`.

This document is self-contained: every field, default, validation rule, and response shape below is verified against the current Mockingbird server code on `main`. You should not need to read source to create a task correctly.

## When to use

- User asks to generate test/synthetic data (CSV, Parquet, or mydumper-style SQL)
- User wants to fill an S3/GCS/Ksyun bucket or local path with data files
- User provides a CREATE TABLE schema and wants data generated from it
- User wants to **customize** column generation with Go code (e.g. derived columns, formula fields, deterministic patterns) — see "Custom Go column generators" below

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/create` | Submit a generation task |
| POST | `/api/scaffold` | Generate a Go template file from CREATE TABLE (for custom generators) |
| POST | `/api/ai-generator-assist` | Have an LLM write/update a Go generator file |
| POST | `/api/validate-generators` | Validate a Go generator file via `go build` (requires server `-workspace`) |
| POST | `/api/ai-assist` | Have an LLM modify or create a CREATE TABLE statement |
| GET  | `/api/status?id={task_id}` | Status of a single task |
| GET  | `/api/tasks` | 10 most recent tasks |
| POST | `/api/cancel?id={task_id}` | Cancel a `pending`/`launching`/`running` task |

## POST /api/create

Submit a task. The server inserts a row in `pending` state; an EC2 launcher (for `target=ec2`) or the local worker picks it up.

### Request body

```json
{
  "sql":           "CREATE TABLE schema.table ( ... );",
  "generators_go": "package user\n\n// gen:column id\nfunc Id(ctx *gen.Ctx) any { ... }\n",
  "path":          "s3://bucket/prefix" | "gcs://bucket/prefix" | "/local/path",
  "start_fileno":  0,
  "end_fileno":    100,
  "rows":          60000,
  "format":        "csv",
  "folders":       0,
  "target":        "local",
  "ksyun":         false,
  "csv":      { "separator": ",", "endline": "\n", "base64": false, "compression": "" },
  "parquet":  { "row_groups": 1, "page_size": "1MiB", "compression": "zstd" },
  "s3":       { "region": "...", "access_key": "...", "secret_key": "...", "provider": "...", "endpoint": "...", "force": false, "role_arn": "" },
  "gcs":      { "credential": "..." }
}
```

### Field reference

**Required:**

| Field | Type | Notes |
|-------|------|-------|
| `sql` | string | Must start with `CREATE TABLE schema.table (...)`. The qualified name is mandatory — the `prefix` (filename stem) is auto-derived as `schema.table`. Do **not** pass `prefix` separately; there is no such API field. |
| `path` | string | Output destination. `s3://...`, `gcs://...`, or absolute local path. S3 credentials may be embedded as query params: `s3://bucket/prefix?access-key=...&secret-access-key=...&region=...&endpoint=...&provider=...&force-path-style=...`. |

**Optional (with real defaults from server code):**

| Field | Type | Default | Validation |
|-------|------|---------|------------|
| `start_fileno` | int | `0` | — |
| `end_fileno` | int | **none** — must satisfy `end_fileno > start_fileno` | rejected if not strictly greater |
| `rows` | int | `60000` (applied when omitted or `0`) | must be `> 0` |
| `format` | string | `"csv"` | must be `"csv"`, `"parquet"`, or `"sql"` (case-insensitive) |
| `folders` | int | `0` (single flat directory) | must be `>= 0`. When `>= 2`, files are sharded across `partNNNNN/` subdirs (5-digit zero-padded) by `fileID % folders`. `0` and `1` both produce a flat layout. |
| `target` | string | `"local"` | any value other than `"ec2"` is treated as `"local"`. `"ec2"` runs the job on EC2 spot workers in the **compute-dev AWS account**. EC2 workers can only reach AWS S3 (compute-dev's own buckets by IAM, or other accounts via `s3.role_arn`); they **cannot** reach KsyunCloud (no `KSYUN_KEY` env, and the internal endpoint is unreachable from AWS) or local-filesystem paths. See "Choosing target + storage" below. |
| `generators_go` | string | unset | Custom Go column generators. **Requires `target=ec2`.** Must parse as valid Go (server runs `go/parser`); for full build validation use `/api/validate-generators` first. With parquet output, `rows / row_groups <= 2_000_000` is enforced. See "Custom Go column generators" below. |
| `ksyun` | bool | `false` | When `true`, the server appends `KSYUN_KEY` query params (credentials + internal endpoint) to `path`. **Must be used with `target=local`** — the EC2 workers don't have the env var and can't reach the Ksyun internal endpoint. |
| `csv` | object | see below | only relevant when `format="csv"` |
| `parquet` | object | see below | only relevant when `format="parquet"` |
| `s3` | object | unset | mutually exclusive with `gcs`. Not needed for EC2 tasks against compute-dev S3 (IAM role). For cross-account S3, set `role_arn`. |
| `gcs` | object | unset | mutually exclusive with `s3` |

**`csv` object:**

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `separator` | string | `","` | |
| `endline` | string | `"\n"` | |
| `base64` | bool | `false` | When `true`, BLOB-like columns are base64-encoded |
| `compression` | string | `""` (no compression) | Allowed: `"zst"` → `.csv.zst`, `"gz"` → `.csv.gz`, or `""`. Any other value is rejected. |

**`parquet` object:**

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `row_groups` | int | `1` | must divide `rows` exactly when `format="parquet"` |
| `page_size` | string | `"1MiB"` | Human-readable size (`"4KiB"`, `"1MiB"`, `"16MiB"`, …). |
| `compression` | string | `"zstd"` | |

**`s3` object:** `region`, `access_key`, `secret_key`, `provider`, `endpoint`, `force` (force path-style), `role_arn` (cross-account assume).

**`gcs` object:** `credential` (path to JSON key file on the worker).

### Choosing target + storage

`target` (where the work runs) and `path` (where the output lands) are **independent** but **not all combinations work**. Pick from this matrix:

| Storage destination | `target` | `ksyun` | `path` | Other |
|---------------------|----------|---------|--------|-------|
| AWS S3, compute-dev account (own buckets) | `"ec2"` (preferred for ≥100 GB) **or** `"local"` | `false` | `s3://my-bucket/prefix` | EC2 uses IAM role; local needs creds in env / `s3` block / path query params |
| AWS S3, **other** AWS account | `"ec2"` | `false` | `s3://their-bucket/prefix` | Owner provides a cross-account role; pass it as `s3.role_arn` |
| KsyunCloud (KS3) | **`"local"` (required)** | **`true`** | `s3://ksyun-bucket/prefix` (the `s3://` scheme is correct — KS3 is S3-API-compat) | Server appends `KSYUN_KEY` automatically. Do **not** also pass `s3` block. |
| GCS | `"local"` | `false` | `gcs://my-bucket/prefix` | Provide service-account JSON path via `gcs.credential` |
| Local filesystem | `"local"` | `false` | `/abs/path` | Writes on the machine running mockingbird (the same one this API runs on). Don't use this for shared / multi-machine consumption. |

**Common mistake — Ksyun + EC2:** if the user asks to "generate to 金山云 / Ksyun / KS3", the call is `target=local` + `ksyun=true`. `target=ec2` with `ksyun=true` will fail (silently or with a connection error) because EC2 workers in compute-dev cannot reach the Ksyun internal endpoint and have no `KSYUN_KEY`.

**When in doubt about target:** if the storage matrix above allows `"local"`, default to `"local"`. Use `"ec2"` only for AWS S3 destinations where output size is large (≥100 GB) or where you want compute-dev's IAM to handle credentials.

### Server-forced behavior (not configurable via API)

- `use_streaming_mode` is always `true`.
- `prefix` is always derived from the SQL schema name; there is no override.
- A sidecar `{prefix}-schema.sql` containing the original CREATE TABLE is written next to the data files automatically.
- `UNSIGNED` integer columns generate non-negative values (the framework respects the `UNSIGNED` flag).

### Response

```json
{ "task_id": 163 }
```

Errors return `{"error": "..."}` with HTTP 4xx/5xx.

### Output file layout

```
{path}/
  {prefix}-schema.sql
  {prefix}.{fileID}.{ext}                  # when folders <= 1
  partNNNNN/{prefix}.{fileID}.{ext}        # when folders >= 2, NNNNN = fileID % folders
```

`{ext}` resolves to:

| `format` | `csv.compression` | Extension | Content |
|----------|-------------------|-----------|---------|
| `csv` (default) | `""` | `csv` | One row per line, configurable separator/endline |
| `csv` | `"zst"` | `csv.zst` | Same, zstd-compressed |
| `csv` | `"gz"` | `csv.gz` | Same, gzip-compressed |
| `parquet` | n/a | `parquet` | Columnar, zstd-compressed by default |
| `sql` | n/a | `sql` | mydumper-compatible: `INSERT INTO` `` `db` `` `.` `` `tbl` `` `VALUES (...),(...);` per file. Numerics emitted unquoted; everything else single-quoted with `\\`, `\'`, `\0`, `\n`, `\r`, `\Z` escaping. |

`{fileID}` runs from `start_fileno` (inclusive) to `end_fileno` (exclusive).

## SQL schema rules

### CREATE TABLE requirements

- **Must** be qualified: `CREATE TABLE schema.table (...)`. Unqualified names are rejected.
- Single-column `PRIMARY KEY` only. Multi-column primary keys are not supported.
- Prefer `BIGINT` over smaller integer types unless explicitly requested.
- Use `UNSIGNED` when you need non-negative ranges; the generator honors it.

### Column COMMENT options

Per-column data generation is controlled by SQL `COMMENT` clauses. Combine multiple options with commas: `COMMENT 'mean=100, stddev=15'`.

| Option | Applies to | Description |
|--------|-----------|-------------|
| `max_length=N` | string types | Max length for CHAR/VARCHAR/TEXT |
| `min_length=N` | string types | Min length (default: 75% of `max_length`) |
| `mean=N` | numeric types | Mean for normal-ish distribution |
| `stddev=N` | numeric types | Std deviation |
| `null_percent=N` | any | Percentage of NULL values (0–100) |
| `compress=N` | string types | Compressibility hint (1–100, clamped). `100` = fully random (default). Lower N replaces `(100−N)%` of bytes with repeated `'a'`, making the data more compressible. |
| `set=[...]` | any | Allowed values as JSON array, e.g. `set=["a","b"]` or `set=[1,2,3]` |
| `order=X` | integer types | `total_order` (strictly increasing — value = absolute rowID), `partial_order` (mostly increasing), `random_order` (default) |

### Mutually exclusive options (rejected if combined)

- `set` cannot combine with `mean` / `stddev` / `order` / `compress` / `max_length` / `min_length` (only `null_percent` is allowed alongside `set`).
- `mean` / `stddev` cannot combine with `order`.

### Defaults & best practices

- Omit COMMENTs unless they carry meaning. The only routinely useful ones are:
  - `max_length` / `min_length` on string columns when you need a specific length;
  - `order=random_order` on non-PK integer columns (this is also the implicit default).
- For a target post-import (RocksDB) compression ratio of ~50% on bulky string columns, `compress=50` on the dominant pad columns is a good starting point.
- For a monotonically increasing primary key across all files, set `COMMENT 'order=total_order'` on the PK column. With `end_fileno=N` and `rows=R`, ids range over `[0, N*R)` with no collisions.

## Custom Go column generators (`generators_go`)

Override the default per-type random generator with arbitrary Go code. Useful for:
- Derived columns (`end_ts = start_ts + Δ`)
- Patterned strings (`order_no = "P" + zero-padded counter`)
- Custom distributions (lognormal, zipf, etc.)
- Cross-column hashes/checksums
- Deterministic data based on `RowID`

### File structure

```go
package user

import (
    "fmt"
    "time"   // optional, only if you generate time.Time columns

    "dataWriter/src/gen"
)

// gen:column id
func Id(ctx *gen.Ctx) any {
    return ctx.RowID + 1            // BIGINT -> int64
}

// gen:column name
func Name(ctx *gen.Ctx) any {
    return fmt.Sprintf("user_%d", ctx.Int64("id"))   // sibling read of an earlier BIGINT column
}

// gen:column created
func Created(ctx *gen.Ctx) any {
    base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
    return base.Add(time.Duration(ctx.RowID) * time.Minute)   // DATETIME -> time.Time
}
```

Rules:
- File MUST start with `package user`.
- File MUST import `"dataWriter/src/gen"`.
- Stdlib only — no third-party packages.
- Each generator function: signature `func <Name>(ctx *gen.Ctx) any`, with `// gen:column <exact_column_name>` directive on the line immediately above. The directive carries the authoritative column name (function name can be any valid Go identifier).
- Emit a function ONLY for columns you want to override. Other columns keep their default generator.
- Never write `main()`, `init()`, or registration calls — the framework does that via `cmd/codegen`.

### Return type by SQL column type (mismatch panics at runtime)

| SQL type | Return type |
|----------|-------------|
| `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` / `YEAR` | `int32` (or `nil` for NULL) |
| `BIGINT` | `int64` (or `nil`) |
| `FLOAT` / `DOUBLE` | `float64` (or `nil`) |
| `CHAR` / `VARCHAR` / `TEXT` / `BLOB` / `TINYBLOB` / `VARBINARY` | `string` (or `nil`) |
| `TIMESTAMP` / `DATETIME` / `DATE` / `TIME` | `time.Time` (or `nil`) |

Returning `nil` produces SQL `NULL`.

### `gen.Ctx` API

| Field/Method | Type | Purpose |
|--------------|------|---------|
| `ctx.RowID` | `int64` | Absolute row number across the whole task. Globally unique and contiguous: `fileNo * rows + rowOffset`. |
| `ctx.Rng` | `*math/rand.Rand` | Worker-local, pre-seeded. Use `ctx.Rng.Int63n(n)`, `Int31n(n)`, `Float64()`, `NormFloat64()`. **Never** use top-level `rand.*`. |
| `ctx.Int32(col)` | `int32` | Read sibling column |
| `ctx.Int64(col)` | `int64` | Read sibling column (BIGINT) |
| `ctx.Float64(col)` | `float64` | Read sibling column |
| `ctx.String(col)` | `string` | Read sibling column |
| `ctx.Time(col)` | `time.Time` | Read sibling column (TIMESTAMP/DATETIME/TIME/DATE) |
| `ctx.IsNull(col)` | `bool` | True iff sibling column was generated as NULL |

**Sibling-read rules** (panic at runtime on violation):
- Only columns that appear EARLIER in `CREATE TABLE` than the current column are readable. Reading a later column panics.
- Wrong accessor type (e.g. `Int64` on a VARCHAR) panics.
- Unknown column name panics.

### Recommended workflow

1. **Scaffold** — `POST /api/scaffold` with `{"sql": "CREATE TABLE ..."}` returns a `generators_go` template with one commented-out function per column.
2. **Edit** locally or via `/api/ai-generator-assist`.
3. **Validate** — `POST /api/validate-generators` runs `go/parser` + codegen + `go build` against the server's workspace and returns a clean error if anything fails. This catches typos, type mismatches, and missing imports before you spend EC2 time.
4. **Submit** — `POST /api/create` with `generators_go` and `target=ec2`.

## The two AI endpoints — choose carefully

This API has **two** distinct LLM-backed endpoints. They are easy to confuse. Always pick based on the artifact you want back:

| If you want... | Call | Input | Output |
|----------------|------|-------|--------|
| A **CREATE TABLE statement** (DDL) — adding/removing/renaming columns, adjusting types, COMMENT options | `POST /api/ai-assist` | `{prompt, sql?}` | `{sql: "CREATE TABLE ..."}` |
| A **`generators_go` Go file** (per-column generator code) — derived columns, custom distributions, patterned strings | `POST /api/ai-generator-assist` | `{sql, prompt, current_code?}` | `{generators_go: "package user ..."}` |

**Rule of thumb:** if your end goal is to change the *schema*, use `ai-assist`. If your end goal is to change *how individual columns are populated*, use `ai-generator-assist`. The `sql` argument means different things in each endpoint:
- For `ai-assist`, `sql` is the **input/seed CREATE TABLE** to be edited.
- For `ai-generator-assist`, `sql` is the **schema context** that informs the Go code (return types, sibling columns) — the schema is NOT modified.

---

### POST /api/ai-assist — Schema editor (CREATE TABLE)

Use when the user describes a schema change in English, e.g. *"add name and email columns"*, *"make id BIGINT AUTO_INCREMENT"*, *"add a status column with set=['ok','err']"*.

```json
POST /api/ai-assist
{
  "sql":    "...optional existing CREATE TABLE...",
  "prompt": "add name and email columns"
}
```

- `prompt` is required.
- `sql` is optional; pass it to refine an existing schema. Omit to create one from scratch (the LLM will pick a reasonable schema name).
- Response: `{ "sql": "CREATE TABLE schema.table (...)" }`. Output is sanitized to a single CREATE TABLE statement.
- The LLM is instructed to follow every "SQL schema rules" constraint in this document (qualified name, single-column PK, BIGINT preference, allowed COMMENT options).

### POST /api/ai-generator-assist — Per-column Go generator code

Use when the user wants to override a column's *value generation logic* with code, e.g. *"make order_no be 'P' followed by a 6-digit counter"*, *"score should follow a lognormal distribution"*, *"end_ts = start_ts + 1-60 random minutes"*.

```json
POST /api/ai-generator-assist
{
  "sql":          "CREATE TABLE app.orders ( ... );",
  "current_code": "...optional existing generators_go file...",
  "prompt":       "make order_no be 'P' followed by a 6-digit zero-padded counter from RowID"
}
```

- `sql` and `prompt` are both required. `sql` is the schema context (NOT modified).
- `current_code` is optional; pass an existing `generators_go` file to refine it.
- Response: `{ "generators_go": "package user\n..." }`. Server runs `go/parser` parse-only validation on the LLM output.
- On parse failure: `400 {"error": "AI output did not parse: ...", "generators_go": "..."}` — the raw output is returned for debugging.
- The LLM emits a function ONLY for columns the prompt explicitly targets. To rewrite all columns, say *"regenerate all columns"* in the prompt.
- The returned `generators_go` is meant to be passed to `/api/validate-generators` (full build check) and then to `/api/create` (`generators_go` field).

### Common confusion to avoid

- ❌ Calling `/api/ai-assist` with prompt *"make id be 1 + RowID"* → you'll get a CREATE TABLE back (LLM may add a COMMENT like `'order=total_order'`), not Go code.
- ❌ Calling `/api/ai-generator-assist` with prompt *"add a status column"* → you'll get a Go file that tries to emit a function for `status`, but the column doesn't exist in `sql`. Use `/api/ai-assist` first to add the column to the schema, then `/api/ai-generator-assist` to override its generator if needed.

---

## POST /api/scaffold

Generates a `generators_go` **template** from the SQL schema (no LLM involved — pure code generation). Each column gets a commented-out stub with the correct return type and directive. Use this as a starting point before hand-editing or before calling `/api/ai-generator-assist` with `current_code`.

```json
POST /api/scaffold
{ "sql": "CREATE TABLE app.orders ( ... );" }
```

Response: `{ "generators_go": "// Code generated by data-writer scaffold...\npackage user\n..." }`.

## POST /api/validate-generators

Runs the full validation pipeline against `generators_go`: parse → codegen → `go build ./cmd/mockingbird-worker`. Catches issues that pure parse-only validation misses (type mismatches, undefined identifiers, missing imports).

```json
POST /api/validate-generators
{ "generators_go": "package user\n..." }
```

Responses:
- `200 {"status": "ok"}` — passes.
- `400 {"error": "parse error: ..."}` — `go/parser` rejected.
- `400 {"error": "codegen failed:\n..."}` — `cmd/codegen` failed.
- `400 {"error": "go build failed:\n..."}` — type/build error, full compiler output included.

**Requires** the server to have been started with `-workspace /path/to/data-writer`. If not, returns `400 {"error": "server was not started with -workspace; validation unavailable"}`. The configured cluster runs with `-workspace` set, so this endpoint is available in production.

**Side effect:** this endpoint **overwrites** `{workspace}/src/user/user_gens.go` with the request payload (and regenerates `registry_gen.go`) to drive the build. The workspace's `src/user/` is treated as ephemeral scratch space — do not keep state there.

## GET /api/status?id={task_id}

```json
{
  "id":            "163",
  "state":         "pending" | "launching" | "running" | "completed" | "failed",
  "progress":      "37%",
  "files_written": 1480,
  "total_files":   4000,
  "written_size":  "402.13 GiB",
  "error":         "",
  "created_at":    "2026-05-08T01:23:45Z",
  "updated_at":    "2026-05-08T01:35:00Z"
}
```

While a task is `running` locally on the server, `files_written` and `written_size` are live (in-memory snapshot, not just the last DB checkpoint).

## GET /api/tasks

Returns the **10 most recent tasks** (newest first). Each entry has the same shape as the status response above plus `target`.

## POST /api/cancel?id={task_id}

Marks a `pending` / `launching` / `running` task as `failed` with `error="cancelled"`. EC2 workers poll task state and exit when they observe `failed`. If the task is running locally on the server, its in-process context is also cancelled. Returns `400` if the task is in any terminal state.

```json
{ "status": "cancelled" }
```

## Example: 100 × 60k CSV rows on EC2

```bash
curl -X POST https://datagen.ingresses.org/api/create \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "CREATE TABLE test.sbtest (\n  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n  k BIGINT NOT NULL DEFAULT 0 COMMENT '\''order=random_order'\'',\n  c CHAR(120) NOT NULL DEFAULT '\'''\'' COMMENT '\''max_length=120, min_length=120'\'',\n  pad CHAR(60) NOT NULL DEFAULT '\'''\'' COMMENT '\''max_length=60, min_length=60'\''\n);",
    "path": "s3://my-bucket/sysbench/",
    "end_fileno": 100,
    "rows": 60000,
    "format": "csv",
    "target": "ec2"
  }'
```

## Example: 1 TB highly compressible CSV, monotonically increasing PK

```bash
curl -X POST https://datagen.ingresses.org/api/create \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "CREATE TABLE test.import_perf_np (id BIGINT UNSIGNED NOT NULL COMMENT '\''order=total_order'\'' PRIMARY KEY, pad VARCHAR(1024) NOT NULL COMMENT '\''max_length=384, min_length=288, compress=50'\'');",
    "path": "s3://m-poc-dataset/e2e/performance/sample/",
    "end_fileno": 4000,
    "rows": 250000,
    "format": "csv",
    "folders": 8,
    "target": "ec2"
  }'
```

## Example: write to KsyunCloud (KS3)

```bash
curl -X POST https://datagen.ingresses.org/api/create \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "CREATE TABLE test.sbtest (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, k BIGINT NOT NULL DEFAULT 0, c CHAR(120) NOT NULL DEFAULT '\'''\'' COMMENT '\''max_length=120, min_length=120'\'');",
    "path": "s3://my-ksyun-bucket/sysbench/",
    "end_fileno": 100,
    "rows": 60000,
    "format": "csv",
    "target": "local",
    "ksyun": true
  }'
```

Key differences from the AWS example: `target="local"` (required for Ksyun — EC2 workers can't reach KS3 internal endpoint) and `ksyun=true` (server injects credentials from its `KSYUN_KEY` env var into the path). Do NOT pass an `s3` block — `ksyun=true` is mutually understood as "use the configured Ksyun credentials".

## Example: custom Go generator (derived end_ts column)

```bash
GENS='{"generators_go":"package user\n\nimport (\n\t\"time\"\n\n\t\"dataWriter/src/gen\"\n)\n\n// gen:column end_ts\nfunc EndTs(ctx *gen.Ctx) any {\n\tstart := ctx.Time(\"start_ts\")\n\treturn start.Add(time.Duration(ctx.Rng.Int63n(60)+1) * time.Minute)\n}\n"}'

# 1. Validate first (catches build errors before EC2)
curl -X POST https://datagen.ingresses.org/api/validate-generators \
  -H 'Content-Type: application/json' -d "$GENS"

# 2. Submit
curl -X POST https://datagen.ingresses.org/api/create \
  -H 'Content-Type: application/json' \
  -d "$(jq -n --argjson g "$GENS" '{
    sql: "CREATE TABLE t.sessions (user_id BIGINT, start_ts TIMESTAMP, end_ts TIMESTAMP);",
    generators_go: $g.generators_go,
    path: "s3://my-bucket/sessions/",
    end_fileno: 100, rows: 60000, format: "csv", target: "ec2"
  }')"
```
