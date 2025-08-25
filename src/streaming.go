package main

import (
	"context"
	"fmt"

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
	targetChunkSizeBytes int
}

// NewChunkSizeCalculator creates a new chunk size calculator
func NewChunkSizeCalculator(targetSizeBytes int) *ChunkSizeCalculator {
	if targetSizeBytes <= 0 {
		targetSizeBytes = 64 * 1024 // Default 64KB
	}
	return &ChunkSizeCalculator{targetChunkSizeBytes: targetSizeBytes}
}

// EstimateRowSize calculates the approximate size of a single row in bytes
func (c *ChunkSizeCalculator) EstimateRowSize(specs []*ColumnSpec, cfg Config) int {
	totalSize := 0
	
	for _, spec := range specs {
		switch spec.SQLType {
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
		case "varchar", "char", "blob":
			// Estimate based on TypeLen, with some overhead for variable length
			if spec.TypeLen > 0 {
				totalSize += spec.TypeLen
			} else {
				totalSize += 32 // Default estimate for variable length strings
			}
		default:
			totalSize += 16 // Default estimate for unknown types
		}
	}
	
	// Add overhead for delimiters (CSV) or encoding (Parquet)
	if cfg.Common.FileFormat == "csv" {
		totalSize += len(specs) - 1 // Commas between fields
		totalSize += 1 // Newline
	} else {
		totalSize = int(float64(totalSize) * 1.2) // 20% overhead for Parquet encoding
	}
	
	return totalSize
}

// CalculateChunkSize determines the optimal number of rows per chunk
func (c *ChunkSizeCalculator) CalculateChunkSize(specs []*ColumnSpec, cfg Config) int {
	rowSize := c.EstimateRowSize(specs, cfg)
	if rowSize <= 0 {
		rowSize = 100 // Fallback
	}
	
	chunkRows := c.targetChunkSizeBytes / rowSize
	if chunkRows < 1 {
		chunkRows = 1
	}
	
	// Ensure reasonable bounds
	minChunkRows := 100
	maxChunkRows := 10000
	
	if chunkRows < minChunkRows {
		chunkRows = minChunkRows
	}
	if chunkRows > maxChunkRows {
		chunkRows = maxChunkRows
	}
	
	return chunkRows
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
func (sc *StreamingCoordinator) fileWriter(ctx context.Context, fileName string, chunkChannel <-chan *FileChunk) error {
	writer, err := sc.store.Create(ctx, fileName, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer writer.Close(ctx)
	
	for chunk := range chunkChannel {
		if len(chunk.Data) > 0 {
			_, err := writer.Write(ctx, chunk.Data)
			if err != nil {
				return errors.Trace(err)
			}
		}
		
		if chunk.IsLast {
			break
		}
	}
	
	return nil
}

// CoordinateStreaming manages the complete streaming process with paired goroutines
func (sc *StreamingCoordinator) CoordinateStreaming(ctx context.Context, startNo, endNo int, specs []*ColumnSpec, cfg Config, writtenFiles interface { Add(delta int32) int32; Load() int32 }, threads int) error {
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
			
			// Create dedicated channel for this file pair
			chunkChannel := make(chan *FileChunk, 10)
			
			// Start writer goroutine for this file
			var writerGroup errgroup.Group
			writerGroup.Go(func() error {
				return sc.fileWriter(ctx, fileName, chunkChannel)
			})
			
			// Generate file in current goroutine, sending chunks to its writer
			err := streamingGenFunc(fileName, fileNo, specs, cfg, chunkChannel)
			close(chunkChannel) // Signal writer that generation is complete
			
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