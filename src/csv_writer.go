package main

import (
	"context"
	"encoding/base64"
	"math/rand"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
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

const CSVChunkSize = 1000 // Process CSV in chunks of 1000 rows

func generateCSVFileStreaming(
	fileName string,
	fileNo int,
	specs []*ColumnSpec,
	cfg Config,
	chunkChannel chan<- *FileChunk,
) error {
	source := rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(65536)))
	rng := rand.New(source)

	startRowID := fileNo * cfg.Common.Rows
	totalRows := cfg.Common.Rows
	
	for rowOffset := 0; rowOffset < totalRows; rowOffset += CSVChunkSize {
		var sb strings.Builder
		chunkRows := CSVChunkSize
		if rowOffset+chunkRows > totalRows {
			chunkRows = totalRows - rowOffset
		}
		
		for i := 0; i < chunkRows; i++ {
			rowID := startRowID + rowOffset + i
			row := generateCSVRow(specs, rowID, cfg.CSV.Base64, rng)
			sb.WriteString(row)
		}
		
		chunk := &FileChunk{
			FileName: fileName,
			Data:     String2Bytes(sb.String()),
			IsLast:   rowOffset+chunkRows >= totalRows,
		}
		
		select {
		case chunkChannel <- chunk:
		default:
			return errors.New("chunk channel full")
		}
	}

	return nil
}
