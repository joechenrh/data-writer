package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

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

func (pw *ParquetWriter) getWriter(w io.Writer, dataPageSize int64, compression compress.Compression) (*file.Writer, error) {
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
		opts = append(opts, parquet.WithCompressionFor(colName, compression))
	}

	node, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return file.NewParquetWriter(w, node, file.WithWriterProps(parquet.NewWriterProperties(opts...))), nil
}

func getParquetCompressionCodec(name string) (compress.Compression, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "snappy":
		return compress.Codecs.Snappy, nil
	case "zstd":
		return compress.Codecs.Zstd, nil
	case "gzip":
		return compress.Codecs.Gzip, nil
	case "brotli":
		return compress.Codecs.Brotli, nil
	case "lz4_raw", "lz4":
		return compress.Codecs.Lz4Raw, nil
	case "uncompressed", "none":
		return compress.Codecs.Uncompressed, nil
	default:
		return compress.Codecs.Uncompressed, fmt.Errorf("unsupported parquet compression: %q", name)
	}
}

func (pw *ParquetWriter) Init(w io.Writer, rows, rowGroups int, dataPageSize int64, specs []*ColumnSpec, compression compress.Compression) error {
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
	pw.w, err = pw.getWriter(w, dataPageSize, compression)
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
		case parquet.Types.FixedLenByteArray:
			pw.valueBufs[i] = make([]parquet.FixedLenByteArray, BatchSize)
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
		case "decimal":
			switch spec.Type {
			case parquet.Types.Int32:
				buf := valueBuffer.([]int32)
				spec.generateDecimalInt32Parquet(rowIDStart, buf, defLevels, pw.rng)
				w, _ := cw.(*file.Int32ColumnChunkWriter)
				num, err = w.WriteBatch(buf, defLevels, nil)
			case parquet.Types.Int64:
				buf := valueBuffer.([]int64)
				spec.generateDecimalInt64Parquet(rowIDStart, buf, defLevels, pw.rng)
				w, _ := cw.(*file.Int64ColumnChunkWriter)
				num, err = w.WriteBatch(buf, defLevels, nil)
			case parquet.Types.FixedLenByteArray:
				buf := valueBuffer.([]parquet.FixedLenByteArray)
				spec.generateDecimalFixedLenParquet(rowIDStart, buf, defLevels, pw.rng)
				w, _ := cw.(*file.FixedLenByteArrayColumnChunkWriter)
				num, err = w.WriteBatch(buf, defLevels, nil)
			default:
				return 0, errors.Errorf("unsupported decimal parquet type: %v", spec.Type)
			}
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
		case "varchar", "char", "blob", "tinyblob":
			buf := valueBuffer.([]parquet.ByteArray)
			spec.generateStringParquet(rowIDStart, buf, defLevels, pw.rng)
			w, _ := cw.(*file.ByteArrayColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "json":
			buf := valueBuffer.([]parquet.ByteArray)
			spec.generateJSONParquet(rowIDStart, buf, defLevels, pw.rng)
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
		case "time":
			buf := valueBuffer.([]int64)
			spec.generateTimestampParquet(buf, defLevels, pw.rng)
			w, _ := cw.(*file.Int64ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case "year":
			buf := valueBuffer.([]int32)
			spec.generateYearParquet(buf, defLevels, pw.rng)
			w, _ := cw.(*file.Int32ColumnChunkWriter)
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
}

// NewParquetGenerator creates a new Parquet generator
func NewParquetGenerator() *ParquetGenerator {
	return &ParquetGenerator{}
}

func (g *ParquetGenerator) GenerateFile(
	ctx context.Context,
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
) error {
	return generateParquetFile(writer, fileNo, specs, cfg)
}

func (g *ParquetGenerator) GenerateFileStreaming(
	ctx context.Context,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
	chunkChannel chan<- *FileChunk,
) error {
	return g.generateParquetFileStreaming(ctx, fileNo, specs, cfg, chunkChannel)
}

// Common parquet generation function that works with any writer
func generateParquetCommon(
	wrapper *writeWrapper,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
) error {
	pw := ParquetWriter{}

	numRows := cfg.Common.Rows
	startRowID := numRows * fileNo
	rowGroups := cfg.Parquet.NumRowGroups
	if numRows%rowGroups != 0 {
		return fmt.Errorf("numRows %d is not divisible by numRowGroups %d", numRows, rowGroups)
	}

	codec, err := getParquetCompressionCodec(cfg.Parquet.Compression)
	if err != nil {
		return err
	}

	if err := pw.Init(wrapper, numRows, rowGroups, int64(cfg.Parquet.PageSizeKB)<<10, specs, codec); err != nil {
		return errors.Trace(err)
	}
	if err := pw.Write(startRowID); err != nil {
		return errors.Trace(err)
	}
	pw.Close()
	return nil
}

func generateParquetFile(
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
) error {
	wrapper := &writeWrapper{Writer: writer}
	return generateParquetCommon(wrapper, fileNo, specs, cfg)
}

func (g *ParquetGenerator) generateParquetFileStreaming(
	ctx context.Context,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
	chunkChannel chan<- *FileChunk,
) error {
	// Create a buffer to capture parquet data
	buffer := &bytes.Buffer{}

	targetChunkSize := 8 << 20 // Default 8MB
	if cfg.Common.ChunkSizeKB > 0 {
		targetChunkSize = cfg.Common.ChunkSizeKB * 1024
	}

	wrapper := &writeWrapper{Writer: &streamingParquetWriter{
		buffer:       buffer,
		chunkChannel: chunkChannel,
		chunkSize:    targetChunkSize,
		ctx:          ctx,
	}}
	return generateParquetCommon(wrapper, fileNo, specs, cfg)
}

// Custom writer for streaming parquet data in chunks
type streamingParquetWriter struct {
	buffer       *bytes.Buffer
	chunkChannel chan<- *FileChunk
	chunkSize    int
	lastSent     int
	ctx          context.Context
}

func (w *streamingParquetWriter) Write(ctx context.Context, data []byte) (int, error) {
	n, err := w.buffer.Write(data)
	if err != nil {
		return n, err
	}

	// Send chunks when buffer reaches chunk size
	for w.buffer.Len()-w.lastSent >= w.chunkSize {
		chunkData := make([]byte, w.chunkSize)
		copy(chunkData, w.buffer.Bytes()[w.lastSent:w.lastSent+w.chunkSize])

		chunk := &FileChunk{
			Data:   chunkData,
			IsLast: false,
		}

		select {
		case w.chunkChannel <- chunk:
			w.lastSent += w.chunkSize
		case <-w.ctx.Done():
			return n, w.ctx.Err()
		}
	}

	// Reset buffer when we've sent enough chunks to prevent memory buildup
	if w.lastSent >= w.chunkSize*4 {
		remaining := w.buffer.Bytes()[w.lastSent:]
		w.buffer.Reset()
		w.buffer.Write(remaining)
		w.lastSent = 0
	}

	return n, nil
}

func (w *streamingParquetWriter) Close(ctx context.Context) error {
	// Send any remaining data
	remaining := w.buffer.Len() - w.lastSent
	if remaining > 0 {
		chunk := &FileChunk{
			Data:   w.buffer.Bytes()[w.lastSent:],
			IsLast: true,
		}
		select {
		case w.chunkChannel <- chunk:
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	} else {
		// Send empty final chunk to signal completion
		chunk := &FileChunk{
			Data:   []byte{},
			IsLast: true,
		}
		select {
		case w.chunkChannel <- chunk:
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
	return nil
}
