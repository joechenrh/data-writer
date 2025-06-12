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

func generateCSVRow(specs []ColumnSpec, rowID int, withBase64 bool, rng *rand.Rand) string {
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
	specs []ColumnSpec,
	cfg Config,
) error {
	// Add some random number to prevent multiple goroutines start simutaneously
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
