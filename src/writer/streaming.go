package writer

import (
	"context"
	"fmt"
	"sync/atomic"

	"dataWriter/src/config"
	"dataWriter/src/spec"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

// Streaming data structure for chunk-based processing
type FileChunk struct {
	Data   []byte
	IsLast bool // Indicates if this is the final chunk for the file
}

// ChunkSizeCalculator for determining optimal chunk sizes
type ChunkSizeCalculator struct {
	cfg *config.Config
}

// NewChunkSizeCalculator creates a new chunk size calculator
func NewChunkSizeCalculator(cfg *config.Config) *ChunkSizeCalculator {
	return &ChunkSizeCalculator{cfg: cfg}
}

// EstimateRowSize calculates the approximate size of a single row in bytes
func (c *ChunkSizeCalculator) EstimateRowSize(specs []*spec.ColumnSpec) int {
	totalSize := 0

	for _, columnSpec := range specs {
		switch columnSpec.SQLType {
		case "bigint", "timestamp", "datetime":
			totalSize += 8 // 8 bytes
		case "int", "mediumint", "date":
			totalSize += 4 // 4 bytes
		case "smallint":
			totalSize += 2 // 2 bytes
		case "tinyint":
			totalSize += 1 // 1 byte
		case "float":
			totalSize += 4 // 4 bytes
		case "double":
			totalSize += 8 // 8 bytes
		case "varchar", "char", "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary", "text", "tinytext", "mediumtext", "longtext":
			// Estimate based on TypeLen, with some overhead for variable length
			if columnSpec.TypeLen > 0 {
				totalSize += columnSpec.TypeLen
			} else {
				totalSize += 32 // Default estimate for variable length strings
			}
		default:
			totalSize += 16 // Default estimate for unknown types
		}
	}

	// Add overhead for delimiters (CSV) or encoding (Parquet)
	if c.cfg.Common.FileFormat == "csv" {
		totalSize += len(specs)
	} else {
		totalSize = int(float64(totalSize) * 1.2) // 20% overhead for Parquet encoding
	}

	return totalSize
}

// CalculateChunkSize determines the optimal number of rows per chunk
func (c *ChunkSizeCalculator) CalculateChunkSize(specs []*spec.ColumnSpec) int {
	rowSize := c.EstimateRowSize(specs)
	if rowSize <= 0 {
		rowSize = 100 // Fallback
	}

	targetSizeBytes := c.cfg.Common.ChunkSizeKB * 1024
	if targetSizeBytes == 0 {
		targetSizeBytes = 32 * units.KiB // Default 32KB
	}

	return max(targetSizeBytes/rowSize, 1)
}

// StreamingCoordinator manages lock-free streaming operations with paired goroutines
type StreamingCoordinator struct {
	store           storage.ExternalStorage
	chunkCalculator ChunkCalculator
}

// NewStreamingCoordinator creates a new streaming coordinator
func NewStreamingCoordinator(store storage.ExternalStorage, chunkCalculator ChunkCalculator) *StreamingCoordinator {
	return &StreamingCoordinator{
		store:           store,
		chunkCalculator: chunkCalculator,
	}
}

// fileWriter handles writing for a single file from its dedicated channel
func (sc *StreamingCoordinator) fileWriter(
	ctx context.Context,
	fileName string,
	chunkChannel <-chan *FileChunk,
	writtenBytes *atomic.Int64,
) error {
	writer, err := sc.store.Create(ctx, fileName, &storage.WriterOption{
		Concurrency: 8,
	})
	if err != nil {
		return errors.Trace(err)
	}
	defer writer.Close(ctx)

	for chunk := range chunkChannel {
		if len(chunk.Data) > 0 {
			n, err := writer.Write(ctx, chunk.Data)
			if err != nil {
				return errors.Trace(err)
			}
			if n > 0 {
				writtenBytes.Add(int64(n))
			}
		}

		if chunk.IsLast {
			break
		}
	}

	return nil
}

// CoordinateStreaming manages the complete streaming process with paired goroutines
func (sc *StreamingCoordinator) CoordinateStreaming(
	ctx context.Context, startNo, endNo int,
	specs []*spec.ColumnSpec,
	cfg config.Config,
	generator DataGenerator,
	suffix string,
	writtenFiles *atomic.Int32,
	writtenBytes *atomic.Int64,
	threads int,
) error {
	// Create a cancellable context for all operations
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var eg errgroup.Group
	eg.SetLimit(threads)

	// Create one generator-writer pair for each file
	for i := startNo; i < endNo; i++ {
		fileNo := i
		eg.Go(func() error {
			fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileNo, suffix)
			if cfg.Common.Folders > 1 {
				fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%cfg.Common.Folders, cfg.Common.Prefix, fileNo, suffix)
			}

			chunkChannel := make(chan *FileChunk, 4)

			// Start writer goroutine for this file
			var writerGroup errgroup.Group
			writerGroup.Go(func() error {
				err := sc.fileWriter(ctx, fileName, chunkChannel, writtenBytes)
				if err != nil {
					// Cancel context on writer error to stop generation
					cancel()
				}
				return err
			})

			// Generate file in current goroutine, sending chunks to its writer
			err := generator.GenerateFileStreaming(ctx, fileNo, specs, cfg, chunkChannel)
			close(chunkChannel)

			if err != nil {
				cancel()
			}

			// Wait for writer to finish
			if writerErr := writerGroup.Wait(); writerErr != nil {
				return writerErr
			}

			if err != nil {
				return err
			}

			writtenFiles.Add(1)
			return nil
		})
	}

	return errors.Trace(eg.Wait())
}
