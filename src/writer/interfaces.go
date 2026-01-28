package writer

import (
	"context"

	"dataWriter/src/config"
	"dataWriter/src/spec"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// DataGenerator interface for file generation
type DataGenerator interface {
	GenerateFile(
		ctx context.Context,
		writer storage.ExternalFileWriter,
		fileNo int,
		specs []*spec.ColumnSpec,
		cfg config.Config,
	) error

	GenerateFileStreaming(
		ctx context.Context,
		fileNo int,
		specs []*spec.ColumnSpec,
		cfg config.Config,
		chunkChannel chan<- *FileChunk,
	) error
}

// ChunkCalculator interface for determining optimal chunk sizes
type ChunkCalculator interface {
	CalculateChunkSize(specs []*spec.ColumnSpec) int
	EstimateRowSize(specs []*spec.ColumnSpec) int
}
