package main

import (
	"context"
	"encoding/base64"
	"math/rand"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/br/pkg/storage"
)

func String2Bytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func generateCSVRow(specs []*ColumnSpec, rowID int, withBase64 bool, rng *rand.Rand) string {
	var sb strings.Builder
	for i, spec := range specs {
		s := generateSingleField(rowID, spec, rng)
		if withBase64 {
			s = base64.StdEncoding.EncodeToString(String2Bytes(s))
		}
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(s)
	}
	sb.WriteByte('\n')
	return sb.String()
}

// CSVGenerator implements DataGenerator interface for CSV files
type CSVGenerator struct {
	chunkCalculator ChunkCalculator
}

// NewCSVGenerator creates a new CSV generator
func NewCSVGenerator(chunkCalculator ChunkCalculator) *CSVGenerator {
	return &CSVGenerator{chunkCalculator: chunkCalculator}
}

func (g *CSVGenerator) GenerateFile(
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
) error {
	return generateCSVFile(writer, fileNo, specs, cfg)
}

func (g *CSVGenerator) GenerateFileStreaming(
	ctx context.Context,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
	chunkChannel chan<- *FileChunk,
) error {
	return g.generateCSVFileStreaming(ctx, fileNo, specs, cfg, chunkChannel)
}

func generateCSVFile(
	writer storage.ExternalFileWriter,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
) error {
	source := rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(65536)))
	rng := rand.New(source)

	startRowID := fileNo * cfg.Common.Rows
	for rowID := startRowID; rowID < startRowID+cfg.Common.Rows; rowID++ {
		row := generateCSVRow(specs, rowID, cfg.CSV.Base64, rng)
		_, err := writer.Write(context.Background(), String2Bytes(row))
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *CSVGenerator) generateCSVFileStreaming(
	ctx context.Context,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
	chunkChannel chan<- *FileChunk,
) error {
	source := rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(65536)))
	rng := rand.New(source)

	startRowID := fileNo * cfg.Common.Rows
	totalRows := cfg.Common.Rows

	// Calculate dynamic chunk size based on row size
	chunkRows := g.chunkCalculator.CalculateChunkSize(specs, cfg)

	for rowOffset := 0; rowOffset < totalRows; rowOffset += chunkRows {
		// Check for context cancellation before processing each chunk
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var sb strings.Builder
		actualChunkRows := chunkRows
		if rowOffset+chunkRows > totalRows {
			actualChunkRows = totalRows - rowOffset
		}

		for i := 0; i < actualChunkRows; i++ {
			rowID := startRowID + rowOffset + i
			row := generateCSVRow(specs, rowID, cfg.CSV.Base64, rng)
			sb.WriteString(row)
		}

		chunk := &FileChunk{
			Data:   String2Bytes(sb.String()),
			IsLast: rowOffset+actualChunkRows >= totalRows,
		}

		// Use context-aware channel send instead of returning error on full channel
		select {
		case chunkChannel <- chunk:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
