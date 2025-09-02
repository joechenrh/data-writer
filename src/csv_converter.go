package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// CSVToParquetConverter handles conversion from CSV to Parquet files
type CSVToParquetConverter struct {
	// Store converted data for writing
	convertedData [][]interface{}
}

// NewCSVToParquetConverter creates a new CSV to Parquet converter
func NewCSVToParquetConverter() *CSVToParquetConverter {
	return &CSVToParquetConverter{}
}

// ConvertCSVToParquet converts a CSV file to Parquet format using the provided schema
func (c *CSVToParquetConverter) ConvertCSVToParquet(csvPath, parquetPath string, specs []*ColumnSpec, cfg Config) error {
	// Read and convert CSV data
	csvData, err := c.readAndConvertCSV(csvPath, specs)
	if err != nil {
		return errors.Trace(err)
	}

	// Create output file writer
	outputFile, err := os.Create(parquetPath)
	if err != nil {
		return errors.Trace(err)
	}
	defer outputFile.Close()

	wrapper := &writeWrapper{Writer: &fileWriter{file: outputFile}}

	// Generate parquet file using converted data
	return c.generateParquetFromData(wrapper, csvData, specs, cfg)
}

// ConvertCSVToParquetStreaming converts a CSV file to storage with streaming
func (c *CSVToParquetConverter) ConvertCSVToParquetStreaming(ctx context.Context, csvPath string, writer storage.ExternalFileWriter, specs []*ColumnSpec, cfg Config) error {
	// Read and convert CSV data
	csvData, err := c.readAndConvertCSV(csvPath, specs)
	if err != nil {
		return errors.Trace(err)
	}

	wrapper := &writeWrapper{Writer: writer}

	// Generate parquet file using converted data
	return c.generateParquetFromData(wrapper, csvData, specs, cfg)
}

// readAndConvertCSV reads CSV file and converts data to appropriate types
func (c *CSVToParquetConverter) readAndConvertCSV(csvPath string, specs []*ColumnSpec) ([][]interface{}, error) {
	csvFile, err := os.Open(csvPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.Comment = '#'
	reader.TrimLeadingSpace = true

	// Read the header row to validate column count
	header, err := reader.Read()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(header) != len(specs) {
		return nil, fmt.Errorf("CSV column count (%d) doesn't match schema column count (%d)", len(header), len(specs))
	}

	var convertedData [][]interface{}

	// Read and convert data rows
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		convertedRow, err := c.convertRow(record, specs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		convertedData = append(convertedData, convertedRow)
	}

	if len(convertedData) == 0 {
		return nil, fmt.Errorf("no data rows found in CSV file")
	}

	return convertedData, nil
}

// convertRow converts a single CSV row to appropriate types
func (c *CSVToParquetConverter) convertRow(record []string, specs []*ColumnSpec) ([]interface{}, error) {
	if len(record) != len(specs) {
		return nil, fmt.Errorf("record column count (%d) doesn't match schema column count (%d)", len(record), len(specs))
	}

	convertedRow := make([]interface{}, len(record))

	for i, value := range record {
		spec := specs[i]
		convertedValue, err := c.convertValue(strings.TrimSpace(value), spec)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value '%s' for column %d (%s): %v", value, i, spec.OrigName, err)
		}
		convertedRow[i] = convertedValue
	}

	return convertedRow, nil
}

// convertValue converts a string value to the appropriate type based on column spec
func (c *CSVToParquetConverter) convertValue(value string, spec *ColumnSpec) (interface{}, error) {
	if value == "" {
		// Handle empty values as null - but problem states no nulls expected
		// So we'll return zero values for each type
		switch spec.SQLType {
		case "bigint":
			return int64(0), nil
		case "int", "mediumint", "smallint", "tinyint":
			return int32(0), nil
		case "float":
			return float32(0), nil
		case "double":
			return float64(0), nil
		case "date":
			return int32(0), nil // Unix epoch
		case "timestamp", "datetime":
			return int64(0), nil // Unix epoch
		case "varchar", "char", "blob":
			return []byte(""), nil
		default:
			return nil, fmt.Errorf("unsupported SQL type: %s", spec.SQLType)
		}
	}

	switch spec.SQLType {
	case "bigint":
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse bigint value '%s': %v", value, err)
		}
		return val, nil

	case "int", "mediumint", "smallint", "tinyint":
		val, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse int value '%s': %v", value, err)
		}
		return int32(val), nil

	case "float":
		val, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float value '%s': %v", value, err)
		}
		return float32(val), nil

	case "double":
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse double value '%s': %v", value, err)
		}
		return val, nil

	case "date":
		date, err := time.Parse("2006-01-02", value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date value '%s': %v", value, err)
		}
		// Convert to days since Unix epoch (1970-01-01)
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int32(date.Sub(epoch).Hours() / 24)
		return days, nil

	case "timestamp", "datetime":
		var timestamp time.Time
		var err error

		// Try different timestamp formats
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05.000",
			time.RFC3339,
		}

		for _, format := range formats {
			timestamp, err = time.Parse(format, value)
			if err == nil {
				break
			}
		}

		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp value '%s': %v", value, err)
		}

		// Convert to microseconds since Unix epoch
		return timestamp.UnixMicro(), nil

	case "varchar", "char", "blob":
		return []byte(value), nil

	default:
		return nil, fmt.Errorf("unsupported SQL type: %s", spec.SQLType)
	}
}

// generateParquetFromData generates a parquet file from converted data
func (c *CSVToParquetConverter) generateParquetFromData(wrapper *writeWrapper, csvData [][]interface{}, specs []*ColumnSpec, cfg Config) error {
	// Create a modified ParquetWriter that uses our converted data
	pw := &CSVParquetWriter{
		csvData: csvData,
	}

	// Initialize with CSV data size
	totalRows := len(csvData)
	rowGroups := cfg.Parquet.NumRowGroups
	if rowGroups <= 0 {
		rowGroups = 1
	}

	dataPageSize := int64(1024 * 1024) // 1MB default
	if cfg.Parquet.PageSizeKB > 0 {
		dataPageSize = cfg.Parquet.PageSizeKB * 1024
	}

	if err := pw.Init(wrapper, totalRows, rowGroups, dataPageSize, specs); err != nil {
		return errors.Trace(err)
	}

	// Write the converted data
	return pw.Write(0)
}

// fileWriter is a simple wrapper for os.File to match the storage interface
type fileWriter struct {
	file *os.File
}

func (fw *fileWriter) Write(ctx context.Context, p []byte) (n int, err error) {
	return fw.file.Write(p)
}

func (fw *fileWriter) Close(ctx context.Context) error {
	return fw.file.Close()
}