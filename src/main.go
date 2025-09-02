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
	operation = flag.String("op", "create", "create/delete/show/convert, default is create")
	sqlPath   = flag.String("sql", "", "sql path")
	cfgPath   = flag.String("cfg", "", "config path")
	threads   = flag.Int("threads", 16, "threads")
	csvPath   = flag.String("csv", "", "path to CSV file for conversion (used with -op convert)")
	outputPath = flag.String("output", "", "output path for converted parquet file (used with -op convert)")
)

var (
	writtenFiles     atomic.Int32
	suffix           string
	streamingGenFunc func(context.Context, int, []*ColumnSpec, Config, chan<- *FileChunk) error
	generator        DataGenerator
)

func main() {
	flag.Parse()

	var config Config
	toml.DecodeFile(*cfgPath, &config)

	// Initialize chunk calculator and generators
	targetChunkSize := 64 * 1024 // Default 64KB
	if config.Common.ChunkSizeKB > 0 {
		targetChunkSize = config.Common.ChunkSizeKB * 1024
	}
	chunkCalculator := NewChunkSizeCalculator(targetChunkSize)

	switch strings.ToLower(config.Common.FileFormat) {
	case "parquet":
		suffix = "parquet"
		generator = NewParquetGenerator(chunkCalculator)
		streamingGenFunc = generator.GenerateFileStreaming
	case "csv":
		suffix = "csv"
		generator = NewCSVGenerator(chunkCalculator)
		streamingGenFunc = generator.GenerateFileStreaming
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
	case "convert":
		if err := ConvertCSVToParquet(config); err != nil {
			log.Fatalf("Failed to convert CSV to Parquet: %v", err)
		}
	default:
		log.Fatalf("Unknown operation: %s", *operation)
	}
}
