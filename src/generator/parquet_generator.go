package generator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"

	"dataWriter/src/config"
	"dataWriter/src/gen"
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

const BatchSize = 1024

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

	// Populated when hasUserCode == true: one whole-row-group buffer per column.
	rgValueBufs []any
	rgDefLevels [][]int16
	hasUserCode bool

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
		// Mark columns that never produce NULLs as Required: maxDefLevel becomes
		// 0 and arrow-go skips the per-row defLevels write path (which evaluates
		// a debug.Assert message via strconv+concat per row, ~30% of CPU before
		// this change).
		repetition := parquet.Repetitions.Optional
		if columnSpec.NullPercent == 0 {
			repetition = parquet.Repetitions.Required
		}
		fields[i], _ = schema.NewPrimitiveNodeConverted(
			colName,
			repetition,
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

func (pw *ParquetWriter) Init(w io.Writer, fileNo, rows, rowGroups int, dataPageSize int64, specs []*spec.ColumnSpec, compression compress.Compression) error {
	pw.rng = rand.New(rand.NewSource(seedForFile(fileNo)))

	pw.numCols = len(specs)
	pw.numRowGroups = rowGroups
	pw.rowsPerRowGroup = rows / rowGroups

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

	// Detect whether any column in this schema has a registered user generator.
	// Use per-column Lookup rather than HasAny() to avoid activating the
	// row-major path when the registry holds generators for a different schema.
	for _, s := range specs {
		if _, ok := gen.Lookup(s.OrigName); ok {
			pw.hasUserCode = true
			break
		}
	}
	if pw.hasUserCode {
		pw.rgValueBufs = make([]any, len(specs))
		pw.rgDefLevels = make([][]int16, len(specs))
		for i, s := range specs {
			pw.rgDefLevels[i] = make([]int16, pw.rowsPerRowGroup)
			switch s.Type {
			case parquet.Types.Int32:
				pw.rgValueBufs[i] = make([]int32, pw.rowsPerRowGroup)
			case parquet.Types.Int64:
				pw.rgValueBufs[i] = make([]int64, pw.rowsPerRowGroup)
			case parquet.Types.FixedLenByteArray:
				pw.rgValueBufs[i] = make([]parquet.FixedLenByteArray, pw.rowsPerRowGroup)
			case parquet.Types.Double:
				pw.rgValueBufs[i] = make([]float64, pw.rowsPerRowGroup)
			case parquet.Types.Float:
				pw.rgValueBufs[i] = make([]float32, pw.rowsPerRowGroup)
			case parquet.Types.ByteArray:
				pw.rgValueBufs[i] = make([]parquet.ByteArray, pw.rowsPerRowGroup)
			default:
				return errors.Errorf("unsupported parquet type for user generator: %v", s.Type)
			}
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

	var (
		written int64
		num     int64
	)

	remaining := pw.rowsPerRowGroup
	for remaining > 0 {
		n := min(BatchSize, remaining)
		defLvls := defLevels[:n]

		// Slice value buffer to the current batch size to make the buffer
		// length match defLvls. Generators key off len(defLevel).
		var sliced any
		switch columnSpec.Type {
		case parquet.Types.Int32:
			sliced = valueBuffer.([]int32)[:n]
		case parquet.Types.Int64:
			sliced = valueBuffer.([]int64)[:n]
		case parquet.Types.FixedLenByteArray:
			sliced = valueBuffer.([]parquet.FixedLenByteArray)[:n]
		case parquet.Types.Double:
			sliced = valueBuffer.([]float64)[:n]
		case parquet.Types.Float:
			sliced = valueBuffer.([]float32)[:n]
		case parquet.Types.ByteArray:
			sliced = valueBuffer.([]parquet.ByteArray)[:n]
		default:
			return 0, errors.Errorf("unsupported parquet writer type: %v", columnSpec.Type)
		}

		if err = columnSpec.FillParquetBatch(rowIDStart, sliced, defLvls, pw.rng); err != nil {
			return written, err
		}

		switch columnSpec.Type {
		case parquet.Types.Int32:
			w, _ := cw.(*file.Int32ColumnChunkWriter)
			num, err = w.WriteBatch(sliced.([]int32), defLvls, nil)
		case parquet.Types.Int64:
			w, _ := cw.(*file.Int64ColumnChunkWriter)
			num, err = w.WriteBatch(sliced.([]int64), defLvls, nil)
		case parquet.Types.FixedLenByteArray:
			w, _ := cw.(*file.FixedLenByteArrayColumnChunkWriter)
			num, err = w.WriteBatch(sliced.([]parquet.FixedLenByteArray), defLvls, nil)
		case parquet.Types.Double:
			w, _ := cw.(*file.Float64ColumnChunkWriter)
			num, err = w.WriteBatch(sliced.([]float64), defLvls, nil)
		case parquet.Types.Float:
			w, _ := cw.(*file.Float32ColumnChunkWriter)
			num, err = w.WriteBatch(sliced.([]float32), defLvls, nil)
		case parquet.Types.ByteArray:
			w, _ := cw.(*file.ByteArrayColumnChunkWriter)
			num, err = w.WriteBatch(sliced.([]parquet.ByteArray), defLvls, nil)
		}

		written += num
		rowIDStart += n
		remaining -= n
		if err != nil {
			return written, err
		}
	}

	return written, err
}

func (pw *ParquetWriter) Write(startRowID int) error {
	for range pw.numRowGroups {
		rgw := pw.w.AppendRowGroup()
		if pw.hasUserCode {
			if err := pw.writeRowGroupRowMajor(rgw, startRowID); err != nil {
				return err
			}
		} else {
			for col := range pw.numCols {
				if _, err := pw.writeNextColumn(rgw, startRowID, col); err != nil {
					return err
				}
			}
		}
		startRowID += pw.rowsPerRowGroup
		rgw.Close()
	}
	return nil
}

// writeRowGroupRowMajor generates an entire row group row-by-row so user
// generators can read sibling columns committed earlier in the same row,
// then flushes column-by-column to the parquet serial writer.
func (pw *ParquetWriter) writeRowGroupRowMajor(rgw file.SerialRowGroupWriter, startRowID int) error {
	names := make([]string, len(pw.specs))
	for i, s := range pw.specs {
		names[i] = s.OrigName
	}

	rowBuf := gen.NewRowBuffer(names)
	for r := 0; r < pw.rowsPerRowGroup; r++ {
		rowID := startRowID + r
		rowBuf.Reset()
		for c, s := range pw.specs {
			valueSlice := pw.rgValueBufs[c]
			if err := s.FillParquetRow(rowID, r, valueSlice, pw.rgDefLevels[c], pw.rng, rowBuf); err != nil {
				return err
			}
		}
	}

	for col, s := range pw.specs {
		cw, err := rgw.NextColumn()
		if err != nil {
			return err
		}
		def := pw.rgDefLevels[col]
		val := pw.rgValueBufs[col]
		switch s.Type {
		case parquet.Types.Int32:
			_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(val.([]int32), def, nil)
		case parquet.Types.Int64:
			_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(val.([]int64), def, nil)
		case parquet.Types.Float:
			_, err = cw.(*file.Float32ColumnChunkWriter).WriteBatch(val.([]float32), def, nil)
		case parquet.Types.Double:
			_, err = cw.(*file.Float64ColumnChunkWriter).WriteBatch(val.([]float64), def, nil)
		case parquet.Types.ByteArray:
			_, err = cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(val.([]parquet.ByteArray), def, nil)
		case parquet.Types.FixedLenByteArray:
			_, err = cw.(*file.FixedLenByteArrayColumnChunkWriter).WriteBatch(val.([]parquet.FixedLenByteArray), def, nil)
		default:
			return errors.Errorf("unsupported parquet type in row-major flush: %v", s.Type)
		}
		cw.Close()
		if err != nil {
			return err
		}
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
	if g.cfg.Common.ChunkSizeBytes > 0 {
		targetChunkSize = g.cfg.Common.ChunkSizeBytes
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

	if err := pw.Init(wrapper, fileNo, numRows, rowGroups, cfg.Parquet.PageSizeBytes, specs, codec); err != nil {
		return errors.Trace(err)
	}
	if err := pw.Write(startRowID); err != nil {
		return errors.Trace(err)
	}
	pw.Close()
	// arrow-go's file.Writer.Close writes the footer via wrapper.Write, but
	// doesn't invoke the streaming flush path. For streaming output the
	// residual buffer (which contains the footer bytes) must be explicitly
	// emitted as the final chunk or the file ends truncated.
	if spw, ok := wrapper.Writer.(*streamingParquetWriter); ok {
		if err := spw.Close(context.Background()); err != nil {
			return errors.Trace(err)
		}
	}
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
