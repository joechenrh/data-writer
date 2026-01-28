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
	"dataWriter/src/generator"
	"dataWriter/src/util"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

func DeleteAllFiles(cfg *config.Config) error {
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

func ShowFiles(cfg *config.Config) error {
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

func GenerateFiles(cfg *config.Config, sqlPath string, threads int) error {
	gen, err := generator.NewFileGenerator(cfg, sqlPath)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.Common.UseStreamingMode {
		return gen.GenerateStreaming(threads)
	}
	return gen.Generate(threads)
}

// UploadLocalFiles uploads all files from a local directory to the configured remote path
func UploadLocalFiles(cfg *config.Config, localDir string, threads int) error {
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
	eg.SetLimit(threads)

	// Progress tracking
	var uploadedFiles atomic.Int32
	go func() {
		bar := util.NewFileProgressBar(len(filesToUpload), "uploading")
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var prev int
		for range ticker.C {
			cur := int(uploadedFiles.Load())
			if cur > prev {
				_ = bar.Add(cur - prev)
				prev = cur
			}
			if cur >= len(filesToUpload) {
				_ = bar.Finish()
				break
			}
		}
	}()

	// Upload each file
	for _, filePath := range filesToUpload {
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
