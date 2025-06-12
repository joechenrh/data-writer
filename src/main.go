package main

import (
	"log"
	"strings"
	"sync/atomic"

	"flag"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/br/pkg/storage"
)

var (
	operation = flag.String("op", "create", "create/delete/show, default is create")
	sqlPath   = flag.String("sql", "", "sql path")
	cfgPath   = flag.String("cfg", "", "config path")
	threads   = flag.Int("threads", 16, "threads")
)

var (
	writtenFiles atomic.Int32
	suffix       string
	genFunc      func(storage.ExternalFileWriter, int, []ColumnSpec, Config) error
)

func main() {
	flag.Parse()

	var config Config
	toml.DecodeFile(*cfgPath, &config)

	switch strings.ToLower(config.Common.FileFormat) {
	case "parquet":
		suffix = "parquet"
		genFunc = generateParquetFile
	case "csv":
		suffix = "csv"
		genFunc = generateCSVFile
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
	default:
		log.Fatalf("Unknown operation: %s", *operation)
	}
}
