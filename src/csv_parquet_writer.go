package main

import (
	"io"
	"math/rand"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/pingcap/errors"
)

// CSVParquetWriter extends ParquetWriter to write CSV data instead of generated data
type CSVParquetWriter struct {
	w               *file.Writer
	defLevels       [][]int16
	valueBufs       []any
	specs           []*ColumnSpec
	csvData         [][]interface{}
	numCols         int
	numRowGroups    int
	rowsPerRowGroup int
	buffer          *memory.Buffer
	rng             *rand.Rand
}

// Init initializes the CSVParquetWriter
func (pw *CSVParquetWriter) Init(w io.Writer, rows, rowGroups int, dataPageSize int64, specs []*ColumnSpec) error {
	source := rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(65536)))
	pw.rng = rand.New(source)

	pw.numCols = len(specs)
	pw.numRowGroups = rowGroups
	pw.rowsPerRowGroup = rows / rowGroups

	// Ensure rowsPerRowGroup is compatible with BatchSize
	if pw.rowsPerRowGroup%BatchSize != 0 {
		// Adjust to be divisible by BatchSize
		pw.rowsPerRowGroup = ((pw.rowsPerRowGroup / BatchSize) + 1) * BatchSize
	}

	var err error

	pw.specs = specs
	pw.defLevels = make([][]int16, len(specs))
	pw.valueBufs = make([]any, len(specs))
	pw.buffer = memory.NewResizableBuffer(memory.DefaultAllocator)
	pw.w, err = pw.getWriter(w, dataPageSize)
	if err != nil {
		return errors.Trace(err)
	}

	for i := range len(specs) {
		pw.defLevels[i] = make([]int16, BatchSize)
		switch specs[i].Type {
		case parquet.Types.Int32:
			pw.valueBufs[i] = make([]int32, BatchSize)
		case parquet.Types.Int64:
			pw.valueBufs[i] = make([]int64, BatchSize)
		case parquet.Types.Float:
			pw.valueBufs[i] = make([]float32, BatchSize)
		case parquet.Types.Double:
			pw.valueBufs[i] = make([]float64, BatchSize)
		case parquet.Types.ByteArray:
			pw.valueBufs[i] = make([]parquet.ByteArray, BatchSize)
		default:
			return errors.Errorf("unsupported parquet type: %v", specs[i].Type)
		}
	}

	return nil
}

// getWriter creates a parquet file writer
func (pw *CSVParquetWriter) getWriter(w io.Writer, dataPageSize int64) (*file.Writer, error) {
	fields := make([]schema.Node, pw.numCols)
	opts := []parquet.WriterProperty{parquet.WithDataPageSize(dataPageSize)}
	for i, spec := range pw.specs {
		colName := spec.OrigName
		fields[i], _ = schema.NewPrimitiveNodeConverted(
			colName,
			parquet.Repetitions.Optional,
			spec.Type, spec.Converted,
			spec.TypeLen, spec.Precision, spec.Scale,
			-1,
		)
		opts = append(opts, parquet.WithDictionaryFor(colName, true))
		opts = append(opts, parquet.WithCompressionFor(colName, compress.Codecs.Snappy))
	}

	node, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return file.NewParquetWriter(w, node, file.WithWriterProps(parquet.NewWriterProperties(opts...))), nil
}

// Write writes the CSV data to parquet file
func (pw *CSVParquetWriter) Write(startRowID int) error {
	totalRows := len(pw.csvData)
	rowsPerGroup := totalRows / pw.numRowGroups
	if totalRows%pw.numRowGroups != 0 {
		rowsPerGroup++
	}

	for groupIdx := 0; groupIdx < pw.numRowGroups; groupIdx++ {
		startRow := groupIdx * rowsPerGroup
		endRow := startRow + rowsPerGroup
		if endRow > totalRows {
			endRow = totalRows
		}

		if startRow >= totalRows {
			break
		}

		rgw := pw.w.AppendRowGroup()

		// Write each column for this row group
		for colIdx := 0; colIdx < pw.numCols; colIdx++ {
			_, err := pw.writeCSVColumn(rgw, startRow, endRow, colIdx)
			if err != nil {
				return errors.Trace(err)
			}
		}

		if err := rgw.Close(); err != nil {
			return errors.Trace(err)
		}
	}

	return pw.w.Close()
}

// writeCSVColumn writes a single column of CSV data
func (pw *CSVParquetWriter) writeCSVColumn(rgw file.SerialRowGroupWriter, startRow, endRow, colIdx int) (int64, error) {
	cw, err := rgw.NextColumn()
	if err != nil {
		return 0, err
	}
	defer cw.Close()

	spec := pw.specs[colIdx]
	rowsToWrite := endRow - startRow
	
	// Process data in batches
	var totalWritten int64
	
	for batchStart := 0; batchStart < rowsToWrite; batchStart += BatchSize {
		batchEnd := batchStart + BatchSize
		if batchEnd > rowsToWrite {
			batchEnd = rowsToWrite
		}
		
		batchSize := batchEnd - batchStart
		
		// Prepare batch data and definition levels
		defLevels := pw.defLevels[colIdx][:batchSize]
		
		// Fill definition levels (assuming no nulls as per requirement)
		for i := 0; i < batchSize; i++ {
			defLevels[i] = 1
		}
		
		// Write based on column type
		var written int64
		switch spec.SQLType {
		case "bigint":
			buf := pw.valueBufs[colIdx].([]int64)[:batchSize]
			for i := 0; i < batchSize; i++ {
				buf[i] = pw.csvData[startRow+batchStart+i][colIdx].(int64)
			}
			w, _ := cw.(*file.Int64ColumnChunkWriter)
			written, err = w.WriteBatch(buf, defLevels, nil)
			
		case "int", "mediumint", "smallint", "tinyint":
			buf := pw.valueBufs[colIdx].([]int32)[:batchSize]
			for i := 0; i < batchSize; i++ {
				buf[i] = pw.csvData[startRow+batchStart+i][colIdx].(int32)
			}
			w, _ := cw.(*file.Int32ColumnChunkWriter)
			written, err = w.WriteBatch(buf, defLevels, nil)
			
		case "float":
			buf := pw.valueBufs[colIdx].([]float32)[:batchSize]
			for i := 0; i < batchSize; i++ {
				buf[i] = pw.csvData[startRow+batchStart+i][colIdx].(float32)
			}
			w, _ := cw.(*file.Float32ColumnChunkWriter)
			written, err = w.WriteBatch(buf, defLevels, nil)
			
		case "double":
			buf := pw.valueBufs[colIdx].([]float64)[:batchSize]
			for i := 0; i < batchSize; i++ {
				buf[i] = pw.csvData[startRow+batchStart+i][colIdx].(float64)
			}
			w, _ := cw.(*file.Float64ColumnChunkWriter)
			written, err = w.WriteBatch(buf, defLevels, nil)
			
		case "varchar", "char", "blob":
			buf := pw.valueBufs[colIdx].([]parquet.ByteArray)[:batchSize]
			for i := 0; i < batchSize; i++ {
				buf[i] = pw.csvData[startRow+batchStart+i][colIdx].([]byte)
			}
			w, _ := cw.(*file.ByteArrayColumnChunkWriter)
			written, err = w.WriteBatch(buf, defLevels, nil)
			
		case "date":
			buf := pw.valueBufs[colIdx].([]int32)[:batchSize]
			for i := 0; i < batchSize; i++ {
				buf[i] = pw.csvData[startRow+batchStart+i][colIdx].(int32)
			}
			w, _ := cw.(*file.Int32ColumnChunkWriter)
			written, err = w.WriteBatch(buf, defLevels, nil)
			
		case "timestamp", "datetime":
			buf := pw.valueBufs[colIdx].([]int64)[:batchSize]
			for i := 0; i < batchSize; i++ {
				buf[i] = pw.csvData[startRow+batchStart+i][colIdx].(int64)
			}
			w, _ := cw.(*file.Int64ColumnChunkWriter)
			written, err = w.WriteBatch(buf, defLevels, nil)
			
		default:
			return 0, errors.Errorf("unsupported column writer type: %s", spec.SQLType)
		}
		
		if err != nil {
			return totalWritten, err
		}
		
		totalWritten += written
	}

	return totalWritten, nil
}