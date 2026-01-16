## data-writer
A self-use tool to generate parquet/CSV

## About this tool
This tool can generate data and write directly into S3(GCS, AWS S3, KS3, etc.).

## Operations

### 1. Create - Generate and upload data
```bash
./bin/parquet-writer -op create -cfg config.toml -sql schema.sql -threads 16
```

### 2. Upload - Upload existing local files to remote storage
```bash
./bin/parquet-writer -op upload -cfg config.toml -dir /path/to/local/directory -threads 16
```
This operation will upload all files from the specified local directory to the path configured in `config.toml`.

### 3. Show - List all files in remote storage
```bash
./bin/parquet-writer -op show -cfg config.toml
```

### 4. Delete - Delete all files from remote storage
```bash
./bin/parquet-writer -op delete -cfg config.toml
```

## TODO
1. Support more data types
2. Test the speed of data generation

## Limitation
- `UNSIGNED`, `DECIMAL` and `YEAR` is not supported yet.