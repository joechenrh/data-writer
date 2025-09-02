## data-writer
A self-use tool to generate parquet/CSV and convert CSV to Parquet

## About this tool
This tool can generate data and write directly into S3(GCS, AWS S3, KS3, etc.).
It can also convert CSV files to Parquet format using SQL schema definitions.

## Features
- Generate CSV and Parquet files from SQL schema
- Convert CSV files to Parquet format with proper type conversion
- Support for various data types (int, bigint, float, double, varchar, date, timestamp)
- Automatic string-to-type conversion based on SQL schema
- Error handling and validation
- Streaming mode for large files

## CSV to Parquet Conversion

### Usage
```bash
./data-writer -op convert -csv input.csv -sql schema.sql -output output.parquet -cfg config.toml
```

### Supported Data Types
- `BIGINT` - Converts string numbers to 64-bit integers
- `INT`, `MEDIUMINT`, `SMALLINT`, `TINYINT` - Converts to 32-bit integers  
- `FLOAT` - Converts to 32-bit floating point
- `DOUBLE` - Converts to 64-bit floating point
- `VARCHAR`, `CHAR`, `BLOB` - Kept as strings/byte arrays
- `DATE` - Converts date strings (YYYY-MM-DD) to Parquet date format
- `TIMESTAMP`, `DATETIME` - Converts timestamp strings to microseconds since epoch

### Date/Time Format Support
The converter supports multiple timestamp formats:
- `2006-01-02 15:04:05`
- `2006-01-02T15:04:05`
- `2006-01-02T15:04:05Z`
- `2006-01-02 15:04:05.000`
- RFC3339 format

### Example

1. Create a SQL schema file (`schema.sql`):
```sql
CREATE TABLE users (
    id BIGINT,
    name VARCHAR(100),
    age INT,
    salary DOUBLE,
    birth_date DATE,
    created_at TIMESTAMP
);
```

2. Create a CSV file (`data.csv`):
```csv
id,name,age,salary,birth_date,created_at
1,"John Doe",30,50000.50,1993-05-15,2023-01-01 10:30:00
2,"Jane Smith",25,60000.75,1998-08-22,2023-01-02T14:45:00
```

3. Create a config file (`config.toml`):
```toml
[common]
format = "parquet"

[parquet]
row_groups = 1
page_size_kb = 1024
```

4. Run the conversion:
```bash
./data-writer -op convert -csv data.csv -sql schema.sql -output output.parquet -cfg config.toml
```

### Error Handling
The tool provides detailed error messages for:
- Column count mismatches between CSV and schema
- Invalid data type conversions
- Malformed date/timestamp values
- Missing required files

## TODO
1. Support more data types
2. Test the speed of data generation

## Limitation
- `UNSIGNED`, `DECIMAL` and `YEAR` is not supported yet.
- Boolean types should be represented as TINYINT (0 or 1)
- CSV files are expected to have no null values (empty values are converted to type defaults)