package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

func deduceTypeForDecimal(precision int) (parquet.Type, int) {
	if precision <= 9 {
		return parquet.Types.Int32, 0
	}
	if precision <= 18 {
		return parquet.Types.Int64, 0
	}

	bits := decimalMaxDigitsBits(precision) + 1
	byteLen := (bits + 7) / 8
	return parquet.Types.FixedLenByteArray, byteLen
}

func decimalMaxDigitsBits(precision int) int {
	if precision <= 0 {
		return 0
	}
	pow10 := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	pow10.Sub(pow10, big.NewInt(1))
	return pow10.BitLen()
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

	fmt.Println("Specs: ", specs)
	fmt.Println("Generating files (direct mode)... ")

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
			if err = generator.GenerateFile(writer, fileNo, specs, cfg); err != nil {
				return errors.Trace(err)
			}
			writtenFiles.Add(1)
			return nil
		})
	}

	return errors.Trace(eg.Wait())
}

// New buffered approach with goroutine separation
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
	coordinator := NewStreamingCoordinator(store, chunkCalculator)

	return coordinator.CoordinateStreaming(ctx, startNo, endNo, specs, cfg, &writtenFiles, *threads)
}

// UploadLocalFiles uploads all files from a local directory to the configured remote path
func UploadLocalFiles(cfg Config, localDir string) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Upload took %s\n", time.Since(start))
	}()

	// Validate local directory exists
	if _, err := os.Stat(localDir); os.IsNotExist(err) {
		return errors.Errorf("local directory does not exist: %s", localDir)
	}

	store, err := GetStore(cfg)
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
			writer, err := store.Create(ctx, remotePath, nil)
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
