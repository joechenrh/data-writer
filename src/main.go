package main

import (
	"context"
	"log"
	"strings"
	"sync/atomic"

	"flag"

	"github.com/BurntSushi/toml"
)

var (
	operation = flag.String("op", "create", "create/delete/show/upload, default is create")
	sqlPath   = flag.String("sql", "", "sql path")
	cfgPath   = flag.String("cfg", "", "config path")
	threads   = flag.Int("threads", 16, "threads")
	localDir  = flag.String("dir", "", "local directory for upload operation")
)

var (
	writtenFiles       atomic.Int32
	suffix             string
	streamingGenerator func(context.Context, int, []*ColumnSpec, Config, chan<- *FileChunk) error
	generator          DataGenerator
)

func main() {
	flag.Parse()

	var config Config
	toml.DecodeFile(*cfgPath, &config)

	chunkCalculator := NewChunkSizeCalculator(&config)

	switch strings.ToLower(config.Common.FileFormat) {
	case "parquet":
		suffix = "parquet"
		generator = NewParquetGenerator()
		streamingGenerator = generator.GenerateFileStreaming
	case "csv":
		suffix = "csv"
		generator = NewCSVGenerator(chunkCalculator)
		streamingGenerator = generator.GenerateFileStreaming
	default:
		log.Fatalf("Unsupported file format: %s", config.Common.FileFormat)
	}

	switch strings.ToLower(*operation) {
	case "delete":
		if err := DeleteAllFiles(config); err != nil {
			log.Fatalf("Failed to delete files: %v", err)
		}
	case "show":
		if err := ShowFiles(config); err != nil {
			log.Fatalf("Failed to show files: %v", err)
		}
	case "create":
		if err := GenerateFiles(config); err != nil {
			log.Fatalf("Failed to generate files: %v", err)
		}
	case "upload":
		if *localDir == "" {
			log.Fatalf("Local directory (-dir) must be specified for upload operation")
		}
		if err := UploadLocalFiles(config, *localDir); err != nil {
			log.Fatalf("Failed to upload files: %v", err)
		}
	default:
		log.Fatalf("Unknown operation: %s", *operation)
	}
}
