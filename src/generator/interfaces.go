package generator

import (
	"context"

	"dataWriter/src/spec"
	"dataWriter/src/util"

	"github.com/pingcap/tidb/br/pkg/storage"
)

type SpecificGenerator interface {
	GenerateOneFile(
		ctx context.Context,
		writer storage.ExternalFileWriter,
		fileNo int,
	) error

	GenerateOneFileStreaming(
		ctx context.Context,
		fileNo int,
		chunkChannel chan<- *util.FileChunk,
	) error
}

// FileGenerator generates files in a specific format.
type FileGenerator interface {
	Generate(threads int) error

	GenerateStreaming(thread int) error
}

// ChunkCalculator interface for determining optimal chunk sizes
type ChunkCalculator interface {
	CalculateChunkSize(specs []*spec.ColumnSpec) int
	EstimateRowSize(specs []*spec.ColumnSpec) int
}
