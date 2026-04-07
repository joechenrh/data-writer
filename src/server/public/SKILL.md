# data-writer

Data generation tool that creates CSV/Parquet files and uploads them to S3, KsyunCloud, or local storage.

Base URL: `https://datagen.ingresses.org`

## API

### Create a task

```
POST /api/create
Content-Type: application/json
```

**Request body:**

```json
{
  "sql": "CREATE TABLE app.users (\n  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n  name VARCHAR(50) COMMENT 'max_length=50',\n  score INT COMMENT 'mean=100, stddev=15'\n);",
  "path": "s3://my-bucket/output/",
  "start_fileno": 0,
  "end_fileno": 100,
  "rows": 60000,
  "format": "csv",
  "target": "ec2"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `sql` | yes | CREATE TABLE statement. **Must use qualified name: `CREATE TABLE schema.table (...)`**. The file prefix is auto-derived from `schema.table`. |
| `path` | yes | Output path: `s3://bucket/prefix`, or local `/path` |
| `start_fileno` | no | Start of file range, inclusive (default: 0) |
| `end_fileno` | no | End of file range, exclusive (default: 100) |
| `rows` | no | Rows per file (default: 60000) |
| `format` | no | `csv` or `parquet` (default: `csv`) |
| `target` | no | `local` or `ec2` (default: `local`). `ec2` runs on a remote AWS instance |
| `folders` | no | Number of subdirectories to spread files into (default: 0, no subdirs) |
| `ksyun` | no | Set `true` for KsyunCloud (credentials injected server-side) |
| `s3` | no | S3 credentials object (see below) |
| `csv` | no | CSV format options (see below) |
| `parquet` | no | Parquet format options (see below) |

**S3 credentials** (optional, not needed for EC2 tasks with IAM role):

```json
{
  "s3": {
    "region": "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "...",
    "provider": "aws",
    "endpoint": ""
  }
}
```

S3 credentials can also be passed as URL query parameters in `path`:
```
s3://bucket/prefix?access-key=AKIA...&secret-access-key=...&region=us-east-1&session-token=...
```

**CSV options** (optional):

```json
{
  "csv": {
    "separator": ",",
    "endline": "\n",
    "base64": false
  }
}
```

**Parquet options** (optional):

```json
{
  "parquet": {
    "compression": "zstd",
    "row_groups": 1,
    "page_size": "1MiB"
  }
}
```

**Response:**

```json
{"task_id": 1}
```

### Check task status

```
GET /api/status?id=1
```

**Response:**

```json
{
  "id": "1",
  "state": "running",
  "progress": "45%",
  "files_written": 45,
  "total_files": 100,
  "written_size": "1.23 GiB",
  "error": "",
  "created_at": "2026-04-03T12:00:00Z",
  "updated_at": "2026-04-03T12:05:00Z"
}
```

States: `pending` -> `launching` -> `running` -> `completed` or `failed`

### List tasks

```
GET /api/tasks
```

Returns the 10 most recent tasks as a JSON array.

### Cancel a task

```
POST /api/cancel?id=1
```

Cancels a pending or running task.

### AI-assisted schema generation

```
POST /api/ai-assist
Content-Type: application/json
```

```json
{
  "sql": "CREATE TABLE t (id INT PRIMARY KEY);",
  "prompt": "add name, email, and age columns with realistic distributions"
}
```

Returns `{"sql": "CREATE TABLE ..."}` with the modified schema.

## Column COMMENT Options

Control data generation behavior per column using SQL COMMENT clauses:

| Option | Description | Example |
|--------|-------------|---------|
| `null_percent` | Percentage of NULL values (0-100) | `COMMENT 'null_percent=20'` |
| `max_length` | Max length for string types | `COMMENT 'max_length=120'` |
| `min_length` | Min length for string types | `COMMENT 'min_length=60'` |
| `mean` | Mean for numeric distributions | `COMMENT 'mean=100'` |
| `stddev` | Standard deviation | `COMMENT 'stddev=15'` |
| `compress` | Compression ratio hint (1-100) | `COMMENT 'compress=40'` |
| `set` | Allowed values as JSON array | `COMMENT 'set=["a","b","c"]'` |
| `order` | `total_order`, `partial_order`, or `random_order` | `COMMENT 'order=partial_order'` |

Multiple options in one comment: `COMMENT 'mean=100, stddev=15'`

**Mutually exclusive options** (rejected if combined):
- `set` cannot be combined with `mean`, `stddev`, `order`, `compress`, `max_length`, or `min_length` (only `null_percent` may accompany `set`).
- `mean`/`stddev` cannot be combined with `order`.

## Examples

### Generate sysbench-like data on EC2

```bash
curl -X POST https://datagen.ingresses.org/api/create \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "CREATE TABLE test.sbtest (\n  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n  k INT NOT NULL DEFAULT 0 COMMENT '\''mean=50000, stddev=25000'\'',\n  c CHAR(120) NOT NULL DEFAULT '\'''\'' COMMENT '\''max_length=120'\'',\n  pad CHAR(60) NOT NULL DEFAULT '\'''\'' COMMENT '\''max_length=60'\''\n);",
    "path": "s3://my-bucket/sysbench/",
    "start_fileno": 0,
    "end_fileno": 100,
    "rows": 60000,
    "format": "csv",
    "target": "ec2"
  }'
```

### Generate with custom distributions

```bash
curl -X POST https://datagen.ingresses.org/api/create \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "CREATE TABLE app.users (\n  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n  name VARCHAR(50) COMMENT '\''max_length=50'\'',\n  status VARCHAR(10) COMMENT '\''set=[\"active\",\"inactive\",\"pending\"]'\'',\n  score INT COMMENT '\''mean=500, stddev=100, order=partial_order'\'',\n  bio TEXT COMMENT '\''max_length=5000, compress=30'\'',\n  nullable_field INT COMMENT '\''null_percent=40'\''\n);",
    "path": "/tmp/test-output/",
    "end_fileno": 10,
    "rows": 10000,
    "format": "parquet",
    "parquet": {"compression": "snappy", "row_groups": 2}
  }'
```

### Poll until completion

```bash
TASK_ID=$(curl -s -X POST https://datagen.ingresses.org/api/create \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE TABLE test.t (id INT PRIMARY KEY);","path":"/tmp/out/","end_fileno":5,"rows":1000}' \
  | jq -r '.task_id')

while true; do
  STATE=$(curl -s "https://datagen.ingresses.org/api/status?id=$TASK_ID" | jq -r '.state')
  echo "Task $TASK_ID: $STATE"
  [ "$STATE" = "completed" ] || [ "$STATE" = "failed" ] && break
  sleep 5
done
```
