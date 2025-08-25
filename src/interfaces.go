package main

import (
	"context"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// DataGenerator interface for file generation
type DataGenerator interface {
	GenerateFile(writer storage.ExternalFileWriter, fileNo int, specs []*ColumnSpec, cfg Config) error
	GenerateFileStreaming(ctx context.Context, fileName string, fileNo int, specs []*ColumnSpec, cfg Config, chunkChannel chan<- *FileChunk) error
}

// ChunkCalculator interface for determining optimal chunk sizes
type ChunkCalculator interface {
	CalculateChunkSize(specs []*ColumnSpec, cfg Config) int
	EstimateRowSize(specs []*ColumnSpec, cfg Config) int
}