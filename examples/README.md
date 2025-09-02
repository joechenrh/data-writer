# CSV to Parquet Conversion Examples

This directory contains complete examples for converting CSV files to Parquet format.

## Files

- `employees_schema.sql` - SQL schema definition for employee data
- `employees_data.csv` - Sample CSV data with employee records
- `convert_config.toml` - Configuration file for the conversion
- `convert_example.sh` - Shell script demonstrating the conversion
- `employees_output.parquet` - Output file (generated after running the example)

## Running the Example

1. Make sure you have built the data-writer tool:
   ```bash
   cd src && go build -o ../data-writer .
   ```

2. Run the example conversion:
   ```bash
   ./examples/convert_example.sh
   ```

   Or run manually:
   ```bash
   ./data-writer -op convert \
     -csv examples/employees_data.csv \
     -sql examples/employees_schema.sql \
     -output examples/employees_output.parquet \
     -cfg examples/convert_config.toml
   ```

## Expected Output

```
Converting employees CSV data to Parquet format...
Converting CSV file examples/employees_data.csv to Parquet file examples/employees_output.parquet using schema with 9 columns
CSV to Parquet conversion took 882.287µs
Conversion successful!
Output file: examples/employees_output.parquet
-rw-r--r-- 1 runner docker 2323 Sep  2 10:27 examples/employees_output.parquet
```

## Data Types Demonstrated

The example includes the following data type conversions:

- `BIGINT` (employee_id) - Large integer values
- `VARCHAR` (first_name, last_name, department) - String values
- `INT` (age) - Regular integer values
- `DOUBLE` (salary) - Floating-point values with decimal places
- `DATE` (hire_date) - Date values in YYYY-MM-DD format
- `TIMESTAMP` (last_update) - Timestamp values with date and time
- `TINYINT` (is_manager) - Small integer for boolean-like values (0/1)

## Customization

You can modify any of the files to test different scenarios:

- Change data types in the SQL schema
- Add or modify CSV data
- Adjust Parquet configuration settings
- Test error handling with invalid data