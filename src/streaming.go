package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// Streaming data structures for chunk-based processing
type FileChunk struct {
	FileName string
	Data     []byte
	IsLast   bool // Indicates if this is the final chunk for the file
}

type FileInfo struct {
	FileName string
	FileNo   int
}

// Writer that streams data directly to storage
type StreamingWriter struct {
	store    storage.ExternalStorage
	writer   storage.ExternalFileWriter
	fileName string
}

func (w *StreamingWriter) Write(ctx context.Context, data []byte) (int, error) {
	return w.writer.Write(ctx, data)
}

func (w *StreamingWriter) Close(ctx context.Context) error {
	return w.writer.Close(ctx)
}

// Buffer writer for compatibility
type BufferWriter struct {
	buffer *bytes.Buffer
}

func (w *BufferWriter) Write(ctx context.Context, data []byte) (int, error) {
	return w.buffer.Write(data)
}

func (w *BufferWriter) Close(ctx context.Context) error {
	return nil
}

// ChunkSizeCalculator implements ChunkCalculator interface
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

// StreamingCoordinator manages streaming operations across multiple files
type StreamingCoordinator struct {
	store            storage.ExternalStorage
	chunkCalculator  ChunkCalculator
	writers          map[string]storage.ExternalFileWriter
	writersMutex     *sync.Mutex
}

// NewStreamingCoordinator creates a new streaming coordinator
func NewStreamingCoordinator(store storage.ExternalStorage, chunkCalculator ChunkCalculator) *StreamingCoordinator {
	return &StreamingCoordinator{
		store:           store,
		chunkCalculator: chunkCalculator,
		writers:         make(map[string]storage.ExternalFileWriter),
		writersMutex:    &sync.Mutex{},
	}
}

// ProcessChunk handles individual file chunks
func (sc *StreamingCoordinator) ProcessChunk(ctx context.Context, chunk *FileChunk) error {
	sc.writersMutex.Lock()
	writer, exists := sc.writers[chunk.FileName]
	
	if !exists {
		newWriter, err := sc.store.Create(ctx, chunk.FileName, nil)
		if err != nil {
			sc.writersMutex.Unlock()
			return errors.Trace(err)
		}
		sc.writers[chunk.FileName] = newWriter
		writer = newWriter
	}
	sc.writersMutex.Unlock()

	if len(chunk.Data) > 0 {
		_, err := writer.Write(ctx, chunk.Data)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if chunk.IsLast {
		sc.writersMutex.Lock()
		writer.Close(ctx)
		delete(sc.writers, chunk.FileName)
		sc.writersMutex.Unlock()
	}
	
	return nil
}

// generateFileStreaming is a generic function for streaming file generation
func generateFileStreaming(fileNo int, specs []*ColumnSpec, cfg Config, chunkChannel chan<- *FileChunk) error {
	fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileNo, suffix)
	if cfg.Common.Folders > 1 {
		fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%cfg.Common.Folders, cfg.Common.Prefix, fileNo, suffix)
	}
	
	return streamingGenFunc(fileName, fileNo, specs, cfg, chunkChannel)
}