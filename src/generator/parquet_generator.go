package generator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/spec"
	"dataWriter/src/util"

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
	specs     []*spec.ColumnSpec

	rng *rand.Rand

	numCols         int
	numRowGroups    int
	rowsPerRowGroup int

	buffer *memory.Buffer
}

func (pw *ParquetWriter) getWriter(w io.Writer, dataPageSize int64, compression compress.Compression) (*file.Writer, error) {
	fields := make([]schema.Node, pw.numCols)
	opts := []parquet.WriterProperty{
		parquet.WithDataPageSize(dataPageSize),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithVersion(parquet.V2_LATEST),
	}
	for i, columnSpec := range pw.specs {
		colName := columnSpec.OrigName
		fields[i], _ = schema.NewPrimitiveNodeConverted(
			colName,
			parquet.Repetitions.Optional,
			columnSpec.Type, columnSpec.Converted,
			columnSpec.TypeLen, columnSpec.Precision, columnSpec.Scale,
			-1,
		)
		encoding, useDict := chooseParquetEncoding(columnSpec)
		opts = append(opts, parquet.WithDictionaryFor(colName, useDict))
		if !useDict {
			opts = append(opts, parquet.WithEncodingFor(colName, encoding))
		}
		opts = append(opts, parquet.WithCompressionFor(colName, compression))
	}

	node, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return file.NewParquetWriter(w, node, file.WithWriterProps(parquet.NewWriterProperties(opts...))), nil
}

func chooseParquetEncoding(columnSpec *spec.ColumnSpec) (parquet.Encoding, bool) {
	hasExplicitSet := len(columnSpec.ValueSet) > 0 || len(columnSpec.IntSet) > 0
	if hasExplicitSet && !columnSpec.IsUnique {
		return parquet.Encodings.Plain, true
	}

	switch columnSpec.Type {
	case parquet.Types.Int32, parquet.Types.Int64:
		if columnSpec.Order == spec.NumericTotalOrder || columnSpec.Order == spec.NumericPartialOrder {
			return parquet.Encodings.DeltaBinaryPacked, false
		}
		return parquet.Encodings.Plain, false
	case parquet.Types.Float, parquet.Types.Double:
		return parquet.Encodings.ByteStreamSplit, false
	case parquet.Types.FixedLenByteArray:
		return parquet.Encodings.ByteStreamSplit, false
	case parquet.Types.ByteArray:
		if columnSpec.IsUnique {
			return parquet.Encodings.Plain, false
		}
		return parquet.Encodings.DeltaLengthByteArray, false
	default:
		return parquet.Encodings.Plain, false
	}
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

func (pw *ParquetWriter) Init(w io.Writer, rows, rowGroups int, dataPageSize int64, specs []*spec.ColumnSpec, compression compress.Compression) error {
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

	columnSpec := pw.specs[currCol]
	defLevels := pw.defLevels[currCol]
	valueBuffer := pw.valueBufs[currCol]
	rounds := pw.rowsPerRowGroup / len(defLevels)

	var (
		written int64
		num     int64
	)

	for range rounds {
		if err = columnSpec.FillParquetBatch(rowIDStart, valueBuffer, defLevels, pw.rng); err != nil {
			return written, err
		}

		switch columnSpec.Type {
		case parquet.Types.Int32:
			buf := valueBuffer.([]int32)
			w, _ := cw.(*file.Int32ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case parquet.Types.Int64:
			buf := valueBuffer.([]int64)
			w, _ := cw.(*file.Int64ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case parquet.Types.FixedLenByteArray:
			buf := valueBuffer.([]parquet.FixedLenByteArray)
			w, _ := cw.(*file.FixedLenByteArrayColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case parquet.Types.Double:
			buf := valueBuffer.([]float64)
			w, _ := cw.(*file.Float64ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case parquet.Types.Float:
			buf := valueBuffer.([]float32)
			w, _ := cw.(*file.Float32ColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		case parquet.Types.ByteArray:
			buf := valueBuffer.([]parquet.ByteArray)
			w, _ := cw.(*file.ByteArrayColumnChunkWriter)
			num, err = w.WriteBatch(buf, defLevels, nil)
		default:
			return 0, errors.Errorf("unsupported parquet writer type: %v", columnSpec.Type)
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

// ParquetGenerator implements FormatGenerator for Parquet files.
type ParquetGenerator struct {
	cfg   *config.Config
	specs []*spec.ColumnSpec
}

func newParquetGenerator(
	cfg *config.Config,
	specs []*spec.ColumnSpec,
) (*ParquetGenerator, error) {
	return &ParquetGenerator{
		cfg:   cfg,
		specs: specs,
	}, nil
}

func (g *ParquetGenerator) FileSuffix() string {
	return "parquet"
}

func (g *ParquetGenerator) GenerateFile(
	ctx context.Context,
	writer storage.ExternalFileWriter,
	fileNo int,
) error {
	wrapper := &writeWrapper{Writer: writer}
	return generateParquetCommon(wrapper, fileNo, g.specs, g.cfg)
}

func (g *ParquetGenerator) GenerateFileStreaming(
	ctx context.Context,
	fileNo int,
	chunkChannel chan<- *util.FileChunk,
) error {
	// Create a buffer to capture parquet data.
	buffer := &bytes.Buffer{}

	targetChunkSize := 8 << 20 // Default 8MB
	if g.cfg.Common.ChunkSizeKB > 0 {
		targetChunkSize = g.cfg.Common.ChunkSizeKB * 1024
	}

	wrapper := &writeWrapper{Writer: &streamingParquetWriter{
		buffer:       buffer,
		chunkChannel: chunkChannel,
		chunkSize:    targetChunkSize,
		ctx:          ctx,
	}}
	return generateParquetCommon(wrapper, fileNo, g.specs, g.cfg)
}

// Common parquet generation function that works with any writer
func generateParquetCommon(
	wrapper *writeWrapper,
	fileNo int,
	specs []*spec.ColumnSpec,
	cfg *config.Config,
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

// Custom writer for streaming parquet data in chunks
type streamingParquetWriter struct {
	buffer       *bytes.Buffer
	chunkChannel chan<- *util.FileChunk
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

		chunk := &util.FileChunk{
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
		chunk := &util.FileChunk{
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
		chunk := &util.FileChunk{
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
