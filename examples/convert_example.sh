#!/bin/bash

# Example script demonstrating CSV to Parquet conversion

echo "Converting employees CSV data to Parquet format..."

# Run the conversion
./data-writer -op convert \
  -csv examples/employees_data.csv \
  -sql examples/employees_schema.sql \
  -output examples/employees_output.parquet \
  -cfg examples/convert_config.toml

if [ $? -eq 0 ]; then
    echo "Conversion successful!"
    echo "Output file: examples/employees_output.parquet"
    ls -la examples/employees_output.parquet
else
    echo "Conversion failed!"
    exit 1
fi