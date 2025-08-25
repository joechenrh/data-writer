package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
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
	genGroup         errgroup.Group
	writeGroup       errgroup.Group
	fileChannels     map[string]chan *FileChunk
	channelsMutex    *sync.RWMutex
}

// NewStreamingCoordinator creates a new streaming coordinator
func NewStreamingCoordinator(store storage.ExternalStorage, chunkCalculator ChunkCalculator, threads int) *StreamingCoordinator {
	genThreads := threads - (threads / 2)
	writeThreads := threads / 2
	if writeThreads == 0 {
		writeThreads = 1
		genThreads = threads - 1
	}
	
	coordinator := &StreamingCoordinator{
		store:           store,
		chunkCalculator: chunkCalculator,
		fileChannels:    make(map[string]chan *FileChunk),
		channelsMutex:   &sync.RWMutex{},
	}
	
	coordinator.genGroup.SetLimit(genThreads)
	coordinator.writeGroup.SetLimit(writeThreads)
	
	return coordinator
}

// getOrCreateFileChannel returns the channel for a specific file, creating it if needed
func (sc *StreamingCoordinator) getOrCreateFileChannel(fileName string) chan *FileChunk {
	sc.channelsMutex.RLock()
	channel, exists := sc.fileChannels[fileName]
	sc.channelsMutex.RUnlock()
	
	if exists {
		return channel
	}
	
	sc.channelsMutex.Lock()
	// Double-check pattern to avoid race condition
	if channel, exists := sc.fileChannels[fileName]; exists {
		sc.channelsMutex.Unlock()
		return channel
	}
	
	channel = make(chan *FileChunk, 10) // Buffer for each file
	sc.fileChannels[fileName] = channel
	
	// Start dedicated writer goroutine for this file
	sc.writeGroup.Go(func() error {
		return sc.handleFileWriter(fileName, channel)
	})
	
	sc.channelsMutex.Unlock()
	return channel
}

// handleFileWriter manages writing for a single file
func (sc *StreamingCoordinator) handleFileWriter(fileName string, chunkChannel <-chan *FileChunk) error {
	ctx := context.Background()
	writer, err := sc.store.Create(ctx, fileName, nil)
	if err != nil {
		return errors.Trace(err)
	}
	
	defer func() {
		writer.Close(ctx)
		// Clean up the channel from the map
		sc.channelsMutex.Lock()
		delete(sc.fileChannels, fileName)
		sc.channelsMutex.Unlock()
	}()
	
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

// ProcessChunk handles individual file chunks using per-file channels
func (sc *StreamingCoordinator) ProcessChunk(ctx context.Context, chunk *FileChunk) error {
	channel := sc.getOrCreateFileChannel(chunk.FileName)
	
	select {
	case channel <- chunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CoordinateStreaming manages the complete streaming process with concurrency
func (sc *StreamingCoordinator) CoordinateStreaming(ctx context.Context, startNo, endNo int, specs []*ColumnSpec, cfg Config, writtenFiles interface { Add(delta int32) int32; Load() int32 }) error {
	chunkChannel := make(chan *FileChunk, (endNo-startNo)*2)
	
	// Start chunk processor that routes chunks to appropriate file channels
	sc.writeGroup.Go(func() error {
		defer func() {
			// Close all file channels when done
			sc.channelsMutex.Lock()
			for _, ch := range sc.fileChannels {
				close(ch)
			}
			sc.channelsMutex.Unlock()
		}()
		
		for chunk := range chunkChannel {
			if err := sc.ProcessChunk(ctx, chunk); err != nil {
				return err
			}
			if chunk.IsLast {
				writtenFiles.Add(1)
			}
		}
		return nil
	})
	
	// Start generator goroutines
	for i := startNo; i < endNo; i++ {
		fileNo := i
		sc.genGroup.Go(func() error {
			return generateFileStreaming(fileNo, specs, cfg, chunkChannel)
		})
	}
	
	// Wait for all generators to complete
	if err := sc.genGroup.Wait(); err != nil {
		close(chunkChannel)
		return errors.Trace(err)
	}
	
	// Close the channel and wait for writers to finish
	close(chunkChannel)
	return errors.Trace(sc.writeGroup.Wait())
}

// generateFileStreaming is a generic function for streaming file generation
func generateFileStreaming(fileNo int, specs []*ColumnSpec, cfg Config, chunkChannel chan<- *FileChunk) error {
	fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileNo, suffix)
	if cfg.Common.Folders > 1 {
		fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%cfg.Common.Folders, cfg.Common.Prefix, fileNo, suffix)
	}
	
	return streamingGenFunc(fileName, fileNo, specs, cfg, chunkChannel)
}