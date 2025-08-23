package main

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// Writer interface for different writer implementations
type Writer interface {
	Write(ctx context.Context, data []byte) (int, error)
	Close(ctx context.Context) error
}

// DataGenerator interface for file generation
type DataGenerator interface {
	GenerateFile(writer storage.ExternalFileWriter, fileNo int, specs []*ColumnSpec, cfg Config) error
	GenerateFileStreaming(fileName string, fileNo int, specs []*ColumnSpec, cfg Config, chunkChannel chan<- *FileChunk) error
}

// ChunkCalculator interface for determining optimal chunk sizes
type ChunkCalculator interface {
	CalculateChunkSize(specs []*ColumnSpec, cfg Config) int
	EstimateRowSize(specs []*ColumnSpec, cfg Config) int
}

// StreamingChunkProcessor handles chunk-based streaming operations
type StreamingChunkProcessor interface {
	ProcessChunks(fileName string, fileNo int, specs []*ColumnSpec, cfg Config, chunkChannel chan<- *FileChunk) error
}