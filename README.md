## data-writer
A self-use tool to generate parquet/CSV

## About this tool
This tool can generate data and write directly into S3(GCS, AWS S3, KS3, etc.).

## Operations

### 1. Create - Generate and upload data
```bash
./bin/data-writer -op create -cfg config.toml -sql schema.sql -threads 16
```

Preview schema specs (with comments applied):
```bash
./bin/data-writer -show-spec -cfg config.toml -sql schema.sql
```

### 2. Upload - Upload existing local files to remote storage
```bash
./bin/data-writer -op upload -cfg config.toml -dir /path/to/local/directory -threads 16
```
This operation will upload all files from the specified local directory to the path configured in `config.toml`.

### 3. Show - List all files in remote storage
```bash
./bin/data-writer -op show -cfg config.toml
```

### 4. Delete - Delete all files from remote storage
```bash
./bin/data-writer -op delete -cfg config.toml
```

## Configuration

Configuration is a TOML file passed via `-cfg`. See `config/sample.toml` for a template or use one of:
- `config/local.toml`
- `config/s3.toml`
- `config/gcs.toml`

Example:
```toml
[common]
path = "/tmp/data-writer"
prefix = "test.t1"
start_fileno = 0
end_fileno = 10
rows = 60000
format = "csv"          # csv or parquet (case-insensitive)
folders = 0             # <=1 means no subfolders
use_streaming_mode = true
chunk_size = "16MiB"     # optional, streaming only

[parquet]
row_groups = 1
page_size = "1MiB"
compression = "zstd"

[csv]
base64 = false
separator = ","
endline = "\n"

# Optional storage credentials (used when path is s3:// or gcs://)
[s3]
region = "us-east-1"
access_key = "AKIA..."
secret_key = "SECRET..."
provider = "aws"
endpoint = "https://s3.amazonaws.com"
force = false
role_arn = ""

[gcs]
credential = "/path/to/service-account.json"
```

Notes:
- `common.path` points to the target storage location (local path or `s3://`/`gcs://`).
- `common.start_fileno` and `common.end_fileno` define a half-open range `[start, end)`.
- `common.folders` splits output into `part%05d/` subfolders when > 1.
- `common.chunk_size` accepts human-readable sizes like `16MiB`. CSV uses it as a target chunk size; Parquet uses it as the raw chunk size (default 8 MiB).
- `parquet.page_size` accepts human-readable sizes like `1MiB` (default 1 MiB).
- `parquet.compression` supports `snappy`, `zstd`, `gzip`, `brotli`, `lz4`, and `none`.

## Column Comment Options

You can customize data generation per column using SQL column comments:

```sql
`c` char(120) NOT NULL DEFAULT '' COMMENT 'max_length=120, min_length=120',
`text_0` text DEFAULT NULL COMMENT 'max_length=20000, min_length=20000, compress=40',
`status` varchar(8) COMMENT 'set=["ok","warn","fail"]',
`score` int COMMENT 'order=total_order, mean=100, stddev=15'
```

Supported options:
- `null_percent`: Percentage of NULL values to generate.
- `max_length`: Maximum length for string types.
- `min_length`: Minimum length for string types.
- `mean`: Mean for numeric distributions.
- `stddev`: Standard deviation for numeric distributions.
- `compress`: Compression ratio hint (1-100).
- `set`: JSON array of allowed values, e.g. `set=["a","b"]` or `set=[1,2,3]`.
- `order`: `total_order`, `partial_order`, or `random_order` for integer ordering.

## Speed

Test with the following schema with 16 threads:

```SQL
CREATE TABLE `test` (
    `id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `k` int NOT NULL DEFAULT '0',
    `c` char(120) NOT NULL DEFAULT '' COMMENT 'max_length=120, min_length=120',
    `pad` char(60) NOT NULL DEFAULT '' COMMENT 'max_length=60, min_length=60',
    `int_0` int NOT NULL DEFAULT '0',
    `int_1` int NOT NULL DEFAULT '0',
    `int_2` int NOT NULL DEFAULT '0',
    `bigint_0` bigint,
    `bigint_1` bigint,
    `bigint_2` bigint,
    `varchar_0` varchar(768) NOT NULL DEFAULT '' COMMENT 'max_length=768, min_length=768',
    `varchar_1` varchar(40) NOT NULL DEFAULT '' COMMENT 'max_length=40, min_length=40',
    `text_0` text DEFAULT NULL COMMENT 'max_length=20000, min_length=20000, compress=40'
);
```

```
2026/01/28 03:15:40 Progress: written files 0 (0.00 files/s), written size 11.25GiB (2303.34 MiB/s)
2026/01/28 03:15:45 Progress: written files 16 (3.20 files/s), written size 22.8GiB (2366.03 MiB/s)
2026/01/28 03:15:50 Progress: written files 16 (0.00 files/s), written size 32.18GiB (1921.72 MiB/s)
```

## Limitation
- `DECIMAL` is not supported for CSV yet.
