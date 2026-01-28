package writer

import (
	"context"
	"encoding/base64"
	"math/rand"
	"time"
	"unsafe"

	"dataWriter/src/config"
	"dataWriter/src/spec"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
)

func string2Bytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

const (
	defaultCSVSeparator = ","
	defaultCSVEndLine   = "\n"
)

func csvSeparatorAndEndline(cfg config.CSVConfig) (string, string) {
	separator := cfg.Separator
	if separator == "" {
		separator = defaultCSVSeparator
	}
	endline := cfg.EndLine
	if endline == "" {
		endline = defaultCSVEndLine
	}
	return separator, endline
}

func generateCSVRow(
	specs []*spec.ColumnSpec,
	rowID int, withBase64 bool,
	rng *rand.Rand, buf []byte,
	separator []byte, endline []byte,
) []byte {
	for i, columnSpec := range specs {
		s := spec.GenerateSingleField(rowID, columnSpec, rng)
		if withBase64 {
			s = base64.StdEncoding.EncodeToString(string2Bytes(s))
		}
		if i > 0 {
			buf = append(buf, separator...)
		}
		buf = append(buf, s...)
	}
	buf = append(buf, endline...)
	return buf
}

// CSVGenerator implements DataGenerator interface for CSV files
type CSVGenerator struct {
	chunkCalculator ChunkCalculator
	separatorBytes  []byte
	endlineBytes    []byte
}

func NewCSVGenerator(chunkCalculator ChunkCalculator, cfg config.CSVConfig) *CSVGenerator {
	separator, endline := csvSeparatorAndEndline(cfg)
	return &CSVGenerator{
		chunkCalculator: chunkCalculator,
		separatorBytes:  []byte(separator),
		endlineBytes:    []byte(endline),
	}
}

func (g *CSVGenerator) GenerateFile(
	ctx context.Context,
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*spec.ColumnSpec,
	cfg config.Config,
) error {
	return generateCSVFile(ctx, writer, fileNo, specs, cfg, g.separatorBytes, g.endlineBytes)
}

func (g *CSVGenerator) GenerateFileStreaming(
	ctx context.Context,
	fileNo int,
	specs []*spec.ColumnSpec,
	cfg config.Config,
	chunkChannel chan<- *FileChunk,
) error {
	return g.generateCSVFileStreaming(ctx, fileNo, specs, cfg, chunkChannel)
}

func generateCSVFile(
	ctx context.Context,
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*spec.ColumnSpec,
	cfg config.Config,
	separatorBytes []byte,
	endlineBytes []byte,
) error {
	var (
		rng        = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(16))))
		buffer     = make([]byte, 0, 64*units.KiB)
		startRowID = fileNo * cfg.Common.Rows
	)

	for i := range cfg.Common.Rows {
		rowID := startRowID + i
		buffer = generateCSVRow(specs, rowID, cfg.CSV.Base64, rng, buffer[:0], separatorBytes, endlineBytes)
		if _, err := writer.Write(ctx, buffer); err != nil {
			return err
		}
	}

	return nil
}

func (g *CSVGenerator) generateCSVFileStreaming(
	ctx context.Context,
	fileNo int,
	specs []*spec.ColumnSpec,
	cfg config.Config,
	chunkChannel chan<- *FileChunk,
) error {
	var (
		rng = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(16))))

		startRowID = fileNo * cfg.Common.Rows
		totalRows  = cfg.Common.Rows

		rowSize    = g.chunkCalculator.EstimateRowSize(specs)
		chunkRows  = g.chunkCalculator.CalculateChunkSize(specs)
		bufferSize = rowSize * chunkRows * 3 / 2
	)

	for rowOffset := 0; rowOffset < totalRows; rowOffset += chunkRows {
		buffer := make([]byte, 0, bufferSize)
		rowsInChunk := min(chunkRows, totalRows-rowOffset)
		isLast := rowOffset+chunkRows >= totalRows

		for i := range rowsInChunk {
			rowID := startRowID + rowOffset + i
			buffer = generateCSVRow(specs, rowID, cfg.CSV.Base64, rng, buffer, g.separatorBytes, g.endlineBytes)
		}

		select {
		case chunkChannel <- &FileChunk{
			Data:   buffer,
			IsLast: isLast,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
