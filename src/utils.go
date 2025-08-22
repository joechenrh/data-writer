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

// Legacy data structure for buffered mode
type FileData struct {
	FileName string
	Content  []byte
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

func generateFileStreaming(fileNo int, specs []*ColumnSpec, cfg Config, chunkChannel chan<- *FileChunk) error {
	fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileNo, suffix)
	if cfg.Common.Folders > 1 {
		fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%cfg.Common.Folders, cfg.Common.Prefix, fileNo, suffix)
	}
	
	return streamingGenFunc(fileName, fileNo, specs, cfg, chunkChannel)
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

	chunkChannel := make(chan *FileChunk, *threads*2)
	
	genThreads := *threads - (*threads / 2)
	writeThreads := *threads / 2
	if writeThreads == 0 {
		writeThreads = 1
		genThreads = *threads - 1
	}

	var genGroup, writeGroup errgroup.Group
	genGroup.SetLimit(genThreads)
	writeGroup.SetLimit(writeThreads)

	// File writers map to track active writers
	writers := make(map[string]storage.ExternalFileWriter)
	writersMutex := &sync.Mutex{}

	for i := 0; i < writeThreads; i++ {
		writeGroup.Go(func() error {
			for chunk := range chunkChannel {
				writersMutex.Lock()
				writer, exists := writers[chunk.FileName]
				
				if !exists {
					newWriter, err := store.Create(ctx, chunk.FileName, nil)
					if err != nil {
						writersMutex.Unlock()
						return errors.Trace(err)
					}
					writers[chunk.FileName] = newWriter
					writer = newWriter
				}
				writersMutex.Unlock()

				if len(chunk.Data) > 0 {
					_, err := writer.Write(ctx, chunk.Data)
					if err != nil {
						return errors.Trace(err)
					}
				}

				if chunk.IsLast {
					writersMutex.Lock()
					writer.Close(ctx)
					delete(writers, chunk.FileName)
					writersMutex.Unlock()
					writtenFiles.Add(1)
				}
			}
			return nil
		})
	}

	for i := startNo; i < endNo; i++ {
		fileNo := i
		genGroup.Go(func() error {
			return generateFileStreaming(fileNo, specs, cfg, chunkChannel)
		})
	}

	if err := genGroup.Wait(); err != nil {
		close(chunkChannel)
		return errors.Trace(err)
	}
	
	close(chunkChannel)
	return errors.Trace(writeGroup.Wait())
}
