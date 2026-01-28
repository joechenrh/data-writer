package generator

import (
	"context"
	"encoding/base64"
	"math/rand"
	"time"
	"unsafe"

	"dataWriter/src/config"
	"dataWriter/src/spec"
	"dataWriter/src/util"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
)

func string2Bytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
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

// CSVGenerator implements FileGenerator interface for CSV files
type CSVGenerator struct {
	*fileGenerator

	chunkCalculator ChunkCalculator
	separatorBytes  []byte
	endlineBytes    []byte
}

func NewCSVGenerator(
	cfg *config.Config,
	sqlPath string,
) (*fileGenerator, error) {
	gen, err := newFileGenerator(cfg, sqlPath)
	if err != nil {
		return nil, err
	}

	separator, endline := util.CSVSeparatorAndEndline(cfg.CSV)
	csvGen := &CSVGenerator{
		fileGenerator:   gen,
		chunkCalculator: util.NewChunkSizeCalculator(cfg),
		separatorBytes:  []byte(separator),
		endlineBytes:    []byte(endline),
	}

	gen.fileSuffix = "csv"
	gen.SpecificGenerator = csvGen
	return gen, nil
}

func (g *CSVGenerator) GenerateOneFile(
	ctx context.Context,
	writer storage.ExternalFileWriter,
	fileNo int,
) error {
	var (
		rng        = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(16))))
		buffer     = make([]byte, 0, 64*units.KiB)
		startRowID = fileNo * g.Config.Common.Rows
	)

	for i := range g.Config.Common.Rows {
		rowID := startRowID + i
		buffer = generateCSVRow(
			g.specs, rowID, g.Config.CSV.Base64, rng,
			buffer[:0], g.separatorBytes, g.endlineBytes)
		if _, err := writer.Write(ctx, buffer); err != nil {
			return err
		}
	}

	return nil
}

func (g *CSVGenerator) GenerateOneFileStreaming(
	ctx context.Context,
	fileNo int,
	chunkChannel chan<- *util.FileChunk,
) error {
	var (
		rng = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(16))))

		startRowID = fileNo * g.Config.Common.Rows
		totalRows  = g.Config.Common.Rows

		specs      = g.specs
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
			buffer = generateCSVRow(
				specs, rowID, g.Config.CSV.Base64, rng,
				buffer, g.separatorBytes, g.endlineBytes)
		}

		select {
		case chunkChannel <- &util.FileChunk{
			Data:   buffer,
			IsLast: isLast,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
