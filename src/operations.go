package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/spec"
	"dataWriter/src/util"
	"dataWriter/src/writer"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

func DeleteAllFiles(cfg config.Config) error {
	var fileNames []string
	store, err := config.GetStore(cfg)
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

func ShowFiles(cfg config.Config) error {
	store, err := config.GetStore(cfg)
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

type countingWriter struct {
	writer   storage.ExternalFileWriter
	progress *util.ProgressLogger
}

func (cw *countingWriter) Write(ctx context.Context, p []byte) (int, error) {
	n, err := cw.writer.Write(ctx, p)
	if n > 0 && cw.progress != nil {
		cw.progress.UpdateBytes(int64(n))
	}
	return n, err
}

func (cw *countingWriter) Close(ctx context.Context) error {
	return cw.writer.Close(ctx)
}

func showProcess(totalFiles int) *util.ProgressLogger {
	return util.NewProgressLogger(totalFiles, "written", 5*time.Second)
}

func GenerateFiles(cfg config.Config) error {
	if cfg.Common.UseStreamingMode {
		return generateFilesStreaming(cfg)
	}
	return generateFilesDirect(cfg)
}

// Original direct writing approach
func generateFilesDirect(cfg config.Config) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s (direct mode)\n", time.Since(start))
	}()

	store, err := config.GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	defer store.Close()

	specs, err := spec.GetSpecFromSQL(*sqlPath)
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()

	fmt.Println("Specs: ", specs)
	fmt.Println("Generating files (direct mode)... ")

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(*threads)

	startNo, endNo := cfg.Common.StartFileNo, cfg.Common.EndFileNo
	progress := showProcess(endNo - startNo)

	for i := startNo; i < endNo; i++ {
		fileNo := i
		eg.Go(func() error {
			fileName := fmt.Sprintf("%s.%d.%s", cfg.Common.Prefix, fileNo, suffix)
			if cfg.Common.Folders > 1 {
				fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%cfg.Common.Folders, cfg.Common.Prefix, fileNo, suffix)
			}

			writer, err := store.Create(ctx, fileName, &storage.WriterOption{
				Concurrency: 8,
			})
			if err != nil {
				return errors.Trace(err)
			}

			writerWithCount := &countingWriter{writer: writer, progress: progress}
			defer writerWithCount.Close(ctx)
			if err = generator.GenerateFile(ctx, writerWithCount, fileNo, specs, cfg); err != nil {
				return errors.Trace(err)
			}
			progress.UpdateFiles(1)
			return nil
		})
	}

	return errors.Trace(eg.Wait())
}

// New buffered approach with goroutine separation
// New streaming approach that processes data in chunks
func generateFilesStreaming(cfg config.Config) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s (streaming mode)\n", time.Since(start))
	}()

	store, err := config.GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	defer store.Close()

	specs, err := spec.GetSpecFromSQL(*sqlPath)
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()

	fmt.Print("Generating files (streaming mode)... ", specs)

	startNo, endNo := cfg.Common.StartFileNo, cfg.Common.EndFileNo
	totalFiles := endNo - startNo
	progress := showProcess(totalFiles)

	// Initialize chunk calculator with configurable size
	chunkCalculator := writer.NewChunkSizeCalculator(&cfg)

	// Log the calculated chunk parameters for visibility
	estimatedRowSize := chunkCalculator.EstimateRowSize(specs)
	chunkRows := chunkCalculator.CalculateChunkSize(specs)
	fmt.Printf("Estimated row size: %d bytes, chunk size: %d rows\n", estimatedRowSize, chunkRows)

	// Create streaming coordinator and let it handle all concurrency
	coordinator := writer.NewStreamingCoordinator(store, chunkCalculator)

	return coordinator.CoordinateStreaming(
		ctx,
		startNo,
		endNo,
		specs,
		cfg,
		generator,
		suffix,
		progress,
		*threads,
	)
}

// UploadLocalFiles uploads all files from a local directory to the configured remote path
func UploadLocalFiles(cfg config.Config, localDir string) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Upload took %s\n", time.Since(start))
	}()

	// Validate local directory exists
	if _, err := os.Stat(localDir); os.IsNotExist(err) {
		return errors.Errorf("local directory does not exist: %s", localDir)
	}

	store, err := config.GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer store.Close()

	// Collect all files to upload
	var filesToUpload []string
	err = filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories
		if info.IsDir() {
			return nil
		}
		filesToUpload = append(filesToUpload, path)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	if len(filesToUpload) == 0 {
		fmt.Println("No files to upload")
		return nil
	}

	fmt.Printf("Found %d files to upload\n", len(filesToUpload))

	ctx := context.Background()
	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(*threads)

	// Progress tracking
	var uploadedFiles atomic.Int32
	go func() {
		bar := progressbar.Default(int64(len(filesToUpload)))
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		var prev int
		for range ticker.C {
			cur := int(uploadedFiles.Load())
			bar.Add(cur - prev)
			prev = cur
			if cur >= len(filesToUpload) {
				break
			}
		}
	}()

	// Upload each file
	for _, localPath := range filesToUpload {
		filePath := localPath
		eg.Go(func() error {
			// Get relative path from localDir
			relPath, err := filepath.Rel(localDir, filePath)
			if err != nil {
				return errors.Trace(err)
			}

			// Convert Windows path separator to Unix-style for remote storage
			remotePath := filepath.ToSlash(relPath)

			// Read local file
			data, err := os.ReadFile(filePath)
			if err != nil {
				return errors.Annotatef(err, "failed to read local file: %s", filePath)
			}

			// Create remote file writer
			writer, err := store.Create(ctx, remotePath, &storage.WriterOption{
				Concurrency: 8,
			})
			if err != nil {
				return errors.Annotatef(err, "failed to create remote file: %s", remotePath)
			}
			defer writer.Close(ctx)

			// Write data
			_, err = writer.Write(ctx, data)
			if err != nil {
				return errors.Annotatef(err, "failed to upload file: %s", remotePath)
			}

			uploadedFiles.Add(1)
			log.Printf("Uploaded: %s -> %s", filePath, remotePath)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}

	fmt.Printf("\nSuccessfully uploaded %d files\n", len(filesToUpload))
	return nil
}
