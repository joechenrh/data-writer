package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"math/rand"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type writeWrapper struct {
	Writer storage.ExternalFileWriter
}

func (self *writeWrapper) Seek(offset int64, pos int) (int64, error) {
	return 0, nil
}

func (self *writeWrapper) Read(b []byte) (int, error) {
	return 0, nil
}

func (self *writeWrapper) Write(b []byte) (int, error) {
	return self.Writer.Write(context.Background(), b)
}

func (self *writeWrapper) Close() error {
	return nil
}

var typeToParquetTypeMap = map[string]parquet.Type{
	"int32":  parquet.Types.Int32,
	"int64":  parquet.Types.Int64,
	"double": parquet.Types.Double,
	"string": parquet.Types.ByteArray,
}

type ParquetWriter struct {
	w         *file.Writer
	defLevels [][]int16
	valueBufs []any
	specs     []ColumnSpec

	rng *rand.Rand

	numCols         int
	numRowGroups    int
	rowsPerRowGroup int

	buffer *memory.Buffer
}

func (pw *ParquetWriter) getWriter(w io.Writer, dataPageSize int64) (*file.Writer, error) {
	fields := make([]schema.Node, pw.numCols)
	opts := []parquet.WriterProperty{parquet.WithDataPageSize(dataPageSize)}
	for i := range pw.numCols {
		colName := pw.specs[i].OrigName
		fields[i], _ = schema.NewPrimitiveNode(
			colName,
			parquet.Repetitions.Optional,
			typeToParquetTypeMap[pw.specs[i].ParquetType],
			-1, int32(pw.specs[i].length),
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

func (pw *ParquetWriter) Init(w io.Writer, rows, rowGroups int, dataPageSize int64, specs []ColumnSpec) error {
	source := rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(65536)))
	pw.rng = rand.New(source)

	pw.numCols = len(specs)
	pw.numRowGroups = rowGroups
	pw.rowsPerRowGroup = rows / rowGroups

	if pw.rowsPerRowGroup%100 != 0 {
		panic("rowsPerRowGroup must be divisible by 100")
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
		pw.defLevels[i] = make([]int16, 100)
		switch specs[i].ParquetType {
		case "int64":
			pw.valueBufs[i] = make([]int64, 100)
		case "float64":
			pw.valueBufs[i] = make([]float64, 100)
		case "string":
			pw.valueBufs[i] = make([]parquet.ByteArray, 100)
		default:
			panic("unimplemented")
		}
	}

	return nil
}

func (pw *ParquetWriter) Close() {
	pw.w.Close()
}

func (pw *ParquetWriter) writeNextColumn(rgw file.SerialRowGroupWriter, rowIDStart, currCol int) (int64, error) {
	cw, err := rgw.NextColumn()
	if err != nil {
		return 0, err
	}
	defer cw.Close()

	spec := pw.specs[currCol]
	defLevels := pw.defLevels[currCol]
	valueBuffer := pw.valueBufs[currCol]
	rounds := pw.rowsPerRowGroup / len(defLevels)

	var (
		written int64
		num     int64
	)

	for range rounds {
		switch w := cw.(type) {
		case *file.Int64ColumnChunkWriter:
			buf := valueBuffer.([]int64)
			spec.generateBatchInt64(rowIDStart, buf, defLevels, pw.rng)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case *file.Float64ColumnChunkWriter:
			buf := valueBuffer.([]float64)
			spec.generateBatchFloat(rowIDStart, buf, defLevels, pw.rng)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case *file.ByteArrayColumnChunkWriter:
			buf := valueBuffer.([]parquet.ByteArray)
			if spec.SQLType == "timestamp" {
				spec.generateBatchTimestamp(buf, defLevels, pw.rng)
			} else {
				spec.generateBatchString(rowIDStart, buf, defLevels, pw.rng)
			}
			num, err = w.WriteBatch(buf, defLevels, nil)
		default:
			return 0, errors.Errorf("unsupported column writer type: %T", w)
		}

		written += num
		rowIDStart += len(defLevels)
		if err != nil {
			return written, err
		}
	}

	return written, err
}

func (pw *ParquetWriter) Write(startRowID int) error {
	for range pw.numRowGroups {
		rgw := pw.w.AppendRowGroup()
		for col := range pw.numCols {
			if _, err := pw.writeNextColumn(rgw, startRowID, col); err != nil {
				return err
			}
		}
		startRowID += pw.rowsPerRowGroup
		rgw.Close()
	}
	return nil
}

func generateParquetFile(
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []ColumnSpec,
	cfg Config,
) error {
	wrapper := writeWrapper{Writer: writer}
	pw := ParquetWriter{}

	numRows := cfg.Common.Rows
	startRowID := numRows * fileNo
	rowGroups := cfg.Parquet.NumRowGroups
	if numRows%rowGroups != 0 {
		return fmt.Errorf("numRows %d is not divisible by numRowGroups %d", numRows, rowGroups)
	}

	if err := pw.Init(&wrapper, numRows, rowGroups, int64(cfg.Parquet.PageSizeKB)<<10, specs); err != nil {
		return errors.Trace(err)
	}
	if err := pw.Write(startRowID); err != nil {
		return errors.Trace(err)
	}
	pw.Close()
	return nil
}
