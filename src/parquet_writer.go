package main

import (
	"bytes"
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

const BatchSize = 50

type writeWrapper struct {
	Writer storage.ExternalFileWriter
}

func (ww *writeWrapper) Seek(offset int64, pos int) (int64, error) {
	return 0, nil
}

func (ww *writeWrapper) Read(b []byte) (int, error) {
	return 0, nil
}

func (ww *writeWrapper) Write(b []byte) (int, error) {
	return ww.Writer.Write(context.Background(), b)
}

func (ww *writeWrapper) Close() error {
	return nil
}

type ParquetWriter struct {
	w         *file.Writer
	defLevels [][]int16
	valueBufs []any
	specs     []*ColumnSpec

	rng *rand.Rand

	numCols         int
	numRowGroups    int
	rowsPerRowGroup int

	buffer *memory.Buffer
}

func (pw *ParquetWriter) getWriter(w io.Writer, dataPageSize int64) (*file.Writer, error) {
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

func (pw *ParquetWriter) Init(w io.Writer, rows, rowGroups int, dataPageSize int64, specs []*ColumnSpec) error {
	source := rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(65536)))
	pw.rng = rand.New(source)

	pw.numCols = len(specs)
	pw.numRowGroups = rowGroups
	pw.rowsPerRowGroup = rows / rowGroups

	if pw.rowsPerRowGroup%BatchSize != 0 {
		panic("rowsPerRowGroup must be divisible by BatchSize")
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
		case parquet.Types.Double:
			pw.valueBufs[i] = make([]float64, BatchSize)
		case parquet.Types.Float:
			pw.valueBufs[i] = make([]float32, BatchSize)
		case parquet.Types.ByteArray:
			pw.valueBufs[i] = make([]parquet.ByteArray, BatchSize)
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
		switch spec.SQLType {
		case "bigint":
			buf := valueBuffer.([]int64)
			spec.generateInt64Parquet(rowIDStart, buf, defLevels, pw.rng)
			w, _ := cw.(*file.Int64ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "int", "mediumint", "smallint", "tinyint":
			buf := valueBuffer.([]int32)
			spec.generateInt32Parquet(rowIDStart, buf, defLevels, pw.rng)
			w, _ := cw.(*file.Int32ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "float":
			buf := valueBuffer.([]float32)
			spec.generateFloat32Parquet(rowIDStart, buf, defLevels, pw.rng)
			w, _ := cw.(*file.Float32ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "double":
			buf := valueBuffer.([]float64)
			spec.generateFloat64Parquet(rowIDStart, buf, defLevels, pw.rng)
			w, _ := cw.(*file.Float64ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "varchar", "char", "blob":
			buf := valueBuffer.([]parquet.ByteArray)
			spec.generateStringParquet(rowIDStart, buf, defLevels, pw.rng)
			w, _ := cw.(*file.ByteArrayColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "date":
			buf := valueBuffer.([]int32)
			spec.generateDateParquet(buf, defLevels, pw.rng)
			w, _ := cw.(*file.Int32ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "timestamp", "datetime":
			buf := valueBuffer.([]int64)
			spec.generateTimestampParquet(buf, defLevels, pw.rng)
			w, _ := cw.(*file.Int64ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		default:
			return 0, errors.Errorf("unsupported column writer type: %s", spec.SQLType)
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

// ParquetGenerator implements DataGenerator interface for Parquet files
type ParquetGenerator struct {
	chunkCalculator ChunkCalculator
}

// NewParquetGenerator creates a new Parquet generator
func NewParquetGenerator(chunkCalculator ChunkCalculator) *ParquetGenerator {
	return &ParquetGenerator{chunkCalculator: chunkCalculator}
}

func (g *ParquetGenerator) GenerateFile(
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
) error {
	return generateParquetFile(writer, fileNo, specs, cfg)
}

func (g *ParquetGenerator) GenerateFileStreaming(
	fileName string,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
	chunkChannel chan<- *FileChunk,
) error {
	return g.generateParquetFileStreaming(fileName, fileNo, specs, cfg, chunkChannel)
}

func generateParquetFile(
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*ColumnSpec,
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

func (g *ParquetGenerator) generateParquetFileStreaming(
	fileName string,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
	chunkChannel chan<- *FileChunk,
) error {
	numRows := cfg.Common.Rows
	startRowID := numRows * fileNo
	rowGroups := cfg.Parquet.NumRowGroups
	if numRows%rowGroups != 0 {
		return fmt.Errorf("numRows %d is not divisible by numRowGroups %d", numRows, rowGroups)
	}

	// Create a buffer to capture parquet data
	buffer := &bytes.Buffer{}
	
	// Calculate dynamic chunk size for Parquet streaming
	targetChunkSize := 64 * 1024 // Default 64KB
	if cfg.Common.ChunkSizeKB > 0 {
		targetChunkSize = cfg.Common.ChunkSizeKB * 1024
	}

	// Stream the parquet data in chunks as it's written
	var lastSent int

	// Custom writer that sends chunks as data is written
	streamWriter := &streamingParquetWriter{
		buffer:       buffer,
		chunkChannel: chunkChannel,
		fileName:     fileName,
		chunkSize:    targetChunkSize,
		lastSent:     &lastSent,
	}
	
	wrapper := writeWrapper{Writer: streamWriter}
	pw := ParquetWriter{}

	if err := pw.Init(&wrapper, numRows, rowGroups, int64(cfg.Parquet.PageSizeKB)<<10, specs); err != nil {
		return errors.Trace(err)
	}

	if err := pw.Write(startRowID); err != nil {
		return errors.Trace(err)
	}
	pw.Close()

	// Send any remaining data
	remaining := buffer.Len() - lastSent
	if remaining > 0 {
		chunk := &FileChunk{
			FileName: fileName,
			Data:     buffer.Bytes()[lastSent:],
			IsLast:   true,
		}
		select {
		case chunkChannel <- chunk:
		default:
			return errors.New("chunk channel full")
		}
	} else {
		// Send empty final chunk to signal completion
		chunk := &FileChunk{
			FileName: fileName,
			Data:     []byte{},
			IsLast:   true,
		}
		select {
		case chunkChannel <- chunk:
		default:
			return errors.New("chunk channel full")
		}
	}

	return nil
}

// Custom writer for streaming parquet data in chunks
type streamingParquetWriter struct {
	buffer       *bytes.Buffer
	chunkChannel chan<- *FileChunk
	fileName     string
	chunkSize    int
	lastSent     *int
}

func (w *streamingParquetWriter) Write(ctx context.Context, data []byte) (int, error) {
	n, err := w.buffer.Write(data)
	if err != nil {
		return n, err
	}

	// Send chunks when buffer reaches chunk size
	for w.buffer.Len()-*w.lastSent >= w.chunkSize {
		chunkData := make([]byte, w.chunkSize)
		copy(chunkData, w.buffer.Bytes()[*w.lastSent:*w.lastSent+w.chunkSize])
		
		chunk := &FileChunk{
			FileName: w.fileName,
			Data:     chunkData,
			IsLast:   false,
		}
		
		select {
		case w.chunkChannel <- chunk:
			*w.lastSent += w.chunkSize
		default:
			return n, errors.New("chunk channel full")
		}
	}

	return n, nil
}

func (w *streamingParquetWriter) Close(ctx context.Context) error {
	return nil
}

// Buffer writer for compatibility
type bufferWriter struct {
	buffer *bytes.Buffer
}

func (w *bufferWriter) Write(ctx context.Context, data []byte) (int, error) {
	return w.buffer.Write(data)
}

func (w *bufferWriter) Close(ctx context.Context) error {
	return nil
}
