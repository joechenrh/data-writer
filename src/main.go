package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"flag"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

var (
	deletePrefixFile = flag.String("deletePrefix", "", "Delete all files with prefix")
	showFiles        = flag.Bool("show", false, "show files")
	sqlPath          = flag.String("sql", "", "sql path")
	cfgPath          = flag.String("cfg", "", "config path")
	threads          = flag.Int("threads", 16, "threads")
)

var (
	writtenFiles atomic.Int32
	suffix       string
	genFunc      func(storage.ExternalFileWriter, int, []ColumnSpec, Config) error
)

func ShowProcess(totalFiles int) {
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

func main() {
	flag.Parse()

	var config Config
	toml.DecodeFile(*cfgPath, &config)

	store, err := GetStore(config)
	if err != nil {
		log.Fatalf(err.Error())
	}

	suffix = "parquet"
	genFunc = generateParquetFile
	if strings.ToLower(config.Common.FileFormat) == "csv" {
		suffix = "csv"
		genFunc = generateCSVFile
	}

	if *deletePrefixFile != "" {
		DeleteAllFilesByPrefix(*deletePrefixFile, config)
		return
	} else if *showFiles {
		ShowFiles(config)
		return
	}

	start := time.Now()
	defer func() {
		fmt.Printf("Generate and upload took %s\n", time.Since(start))
	}()

	specs := getSpecFromSQL(*sqlPath)
	ctx := context.Background()

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(*threads)

	startNo, endNo := config.Common.StartFileNo, config.Common.EndFileNo
	ShowProcess(endNo - startNo)

	for i := startNo; i < endNo; i++ {
		fileNo := i
		eg.Go(func() error {
			fileName := fmt.Sprintf("%s.%d.%s", config.Common.Prefix, fileNo, suffix)
			if config.Common.Folders > 1 {
				fileName = fmt.Sprintf("part%d/%s.%d.%s", fileNo%config.Common.Folders, config.Common.Prefix, fileNo, suffix)
			}

			writer, err := store.Create(ctx, fileName, nil)
			AssertTrue(err)
			AssertTrue(genFunc(writer, fileNo, specs, config))
			AssertTrue(writer.Close(ctx))

			writtenFiles.Add(1)
			return nil
		})
	}

	AssertTrue(eg.Wait())
	store.Close()
}
