package main

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// DataGenerator interface for file generation
type DataGenerator interface {
	GenerateFile(ctx context.Context, writer storage.ExternalFileWriter, fileNo int, specs []*ColumnSpec, cfg Config) error
	GenerateFileStreaming(ctx context.Context, fileNo int, specs []*ColumnSpec, cfg Config, chunkChannel chan<- *FileChunk) error
}

// ChunkCalculator interface for determining optimal chunk sizes
type ChunkCalculator interface {
	CalculateChunkSize(specs []*ColumnSpec) int
	EstimateRowSize(specs []*ColumnSpec) int
}
