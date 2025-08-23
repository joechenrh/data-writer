package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

// Legacy data structure for buffered mode
type FileData struct {
	FileName string
	Content  []byte
}

// Legacy in-memory writer for buffered mode
type InMemoryWriter struct {
	buffer *bytes.Buffer
}

func (w *InMemoryWriter) Write(ctx context.Context, data []byte) (int, error) {
	return w.buffer.Write(data)
}

func (w *InMemoryWriter) Close(ctx context.Context) error {
	return nil
}

// Buffer pool for reusing buffers to reduce memory allocation
var bufferPool = &sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func generateFileData(fileNo int, specs []*ColumnSpec, cfg Config) (*FileData, error) {
	buffer := bufferPool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer bufferPool.Put(buffer)
	
	writer := &InMemoryWriter{buffer: buffer}
	
	if err := genFunc(writer, fileNo, specs, cfg); err != nil {
		return nil, err
	}
	
	fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileNo, suffix)
	if cfg.Common.Folders > 1 {
		fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%cfg.Common.Folders, cfg.Common.Prefix, fileNo, suffix)
	}
	
	// Copy buffer content to avoid issues with buffer reuse
	content := make([]byte, buffer.Len())
	copy(content, buffer.Bytes())
	
	return &FileData{
		FileName: fileName,
		Content:  content,
	}, nil
}

func DeleteAllFiles(cfg Config) error {
	var fileNames []string
	store, err := GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	//nolint: errcheck
	defer store.Close()

	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		fileNames = append(fileNames, path)
		return nil
	})

	var eg errgroup.Group
	eg.SetLimit(runtime.NumCPU())
	for _, fileName := range fileNames {
		f := fileName
		eg.Go(func() error {
			return store.DeleteFile(context.Background(), f)
		})
	}

	return eg.Wait()
}

func ShowFiles(cfg Config) error {
	store, err := GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	//nolint: errcheck
	defer store.Close()

	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		log.Printf("Name: %s, Size: %d, Size (MiB): %f", path, size, float64(size)/1024/1024)
		return nil
	})

	return nil
}

func showProcess(totalFiles int) {
	go func() {
		bar := progressbar.Default(int64(totalFiles))
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		var prev int
		for range ticker.C {
			cur := int(writtenFiles.Load())
			bar.Add(cur - prev)
			prev = cur
			if cur >= totalFiles {
				break
			}
		}
	}()
}

func GenerateFiles(cfg Config) error {
	if cfg.Common.UseStreamingMode {
		return generateFilesStreaming(cfg)
	} else if cfg.Common.UseBufferedWriter {
		return generateFilesBuffered(cfg)
	}
	return generateFilesDirect(cfg)
}

// Original direct writing approach
func generateFilesDirect(cfg Config) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s (direct mode)\n", time.Since(start))
	}()

	store, err := GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	defer store.Close()

	specs, err := getSpecFromSQL(*sqlPath)
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()

	fmt.Print("Generating files (direct mode)... ", specs)

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(*threads)

	startNo, endNo := cfg.Common.StartFileNo, cfg.Common.EndFileNo
	showProcess(endNo - startNo)

	for i := startNo; i < endNo; i++ {
		fileNo := i
		eg.Go(func() error {
			fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileNo, suffix)
			if cfg.Common.Folders > 1 {
				fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%cfg.Common.Folders, cfg.Common.Prefix, fileNo, suffix)
			}

			writer, err := store.Create(ctx, fileName, nil)
			if err != nil {
				return errors.Trace(err)
			}

			defer writer.Close(ctx)
			if err = genFunc(writer, fileNo, specs, cfg); err != nil {
				return errors.Trace(err)
			}
			writtenFiles.Add(1)
			return nil
		})
	}

	return errors.Trace(eg.Wait())
}

// New buffered approach with goroutine separation
func generateFilesBuffered(cfg Config) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s (buffered mode)\n", time.Since(start))
	}()

	store, err := GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	defer store.Close()

	specs, err := getSpecFromSQL(*sqlPath)
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()

	fmt.Print("Generating files (buffered mode)... ", specs)

	startNo, endNo := cfg.Common.StartFileNo, cfg.Common.EndFileNo
	totalFiles := endNo - startNo
	showProcess(totalFiles)

	dataChannel := make(chan *FileData, *threads)
	
	genThreads := *threads - (*threads / 2)
	writeThreads := *threads / 2
	if writeThreads == 0 {
		writeThreads = 1
		genThreads = *threads - 1
	}

	var genGroup, writeGroup errgroup.Group
	genGroup.SetLimit(genThreads)
	writeGroup.SetLimit(writeThreads)

	for i := 0; i < writeThreads; i++ {
		writeGroup.Go(func() error {
			for fileData := range dataChannel {
				writer, err := store.Create(ctx, fileData.FileName, nil)
				if err != nil {
					return errors.Trace(err)
				}

				_, err = writer.Write(ctx, fileData.Content)
				writer.Close(ctx)
				if err != nil {
					return errors.Trace(err)
				}
				writtenFiles.Add(1)
			}
			return nil
		})
	}

	for i := startNo; i < endNo; i++ {
		fileNo := i
		genGroup.Go(func() error {
			fileData, err := generateFileData(fileNo, specs, cfg)
			if err != nil {
				return errors.Trace(err)
			}
			
			select {
			case dataChannel <- fileData:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	if err := genGroup.Wait(); err != nil {
		close(dataChannel)
		return errors.Trace(err)
	}
	
	close(dataChannel)
	return errors.Trace(writeGroup.Wait())
}

// New streaming approach that processes data in chunks
func generateFilesStreaming(cfg Config) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s (streaming mode)\n", time.Since(start))
	}()

	store, err := GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	defer store.Close()

	specs, err := getSpecFromSQL(*sqlPath)
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()

	fmt.Print("Generating files (streaming mode)... ", specs)

	startNo, endNo := cfg.Common.StartFileNo, cfg.Common.EndFileNo
	totalFiles := endNo - startNo
	showProcess(totalFiles)

	// Initialize chunk calculator with configurable size
	targetChunkSize := 64 * 1024 // Default 64KB
	if cfg.Common.ChunkSizeKB > 0 {
		targetChunkSize = cfg.Common.ChunkSizeKB * 1024
	}
	chunkCalculator := NewChunkSizeCalculator(targetChunkSize)

	// Log the calculated chunk parameters for visibility
	estimatedRowSize := chunkCalculator.EstimateRowSize(specs, cfg)
	chunkRows := chunkCalculator.CalculateChunkSize(specs, cfg)
	fmt.Printf("Estimated row size: %d bytes, chunk size: %d rows\n", estimatedRowSize, chunkRows)

	// Create streaming coordinator and let it handle all concurrency
	coordinator := NewStreamingCoordinator(store, chunkCalculator, *threads)
	
	return coordinator.CoordinateStreaming(ctx, startNo, endNo, specs, cfg, &writtenFiles)
}
