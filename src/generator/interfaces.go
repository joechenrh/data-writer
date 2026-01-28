package generator

import (
	"context"

	"dataWriter/src/util"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// FileGenerator generates files for a specific format.
type FileGenerator interface {
	FileSuffix() string

	GenerateFile(
		ctx context.Context,
		writer storage.ExternalFileWriter,
		fileNo int,
	) error

	GenerateFileStreaming(
		ctx context.Context,
		fileNo int,
		chunkChannel chan<- *util.FileChunk,
	) error
}
