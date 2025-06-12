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

type NumericOrder int

const (
	NumericNoOrder NumericOrder = iota
	NumericTotalOrder
	NumericPartialOrder
	NumericRandomOrder
)

type ColumnSpec struct {
	// For string type, it's the maximum length
	// For numeric type, it's the maximum number of digits.
	length    int
	IsUnique  bool
	IsPrimary bool

	OrigName    string
	SQLType     string
	ParquetType string

	NullPercent int

	// Below are used for numeric types
	Order  NumericOrder
	Mean   int
	StdDev int
	Signed bool
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
	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s\n", time.Since(start))
	}()

	store, err := GetStore(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	//nolint: errcheck
	defer store.Close()

	specs := getSpecFromSQL(*sqlPath)
	ctx := context.Background()

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

			//nolint: errcheck
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
