package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

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
