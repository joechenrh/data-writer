package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"sync/atomic"

	"dataWriter/src/config"
	"dataWriter/src/writer"

	"github.com/BurntSushi/toml"
)

var (
	operation  = flag.String("op", "create", "create/delete/show/upload, default is create")
	sqlPath    = flag.String("sql", "", "sql path")
	cfgPath    = flag.String("cfg", "", "config path")
	threads    = flag.Int("threads", 16, "threads")
	localDir   = flag.String("dir", "", "local directory for upload operation")
	cpuProfile = flag.String("cpuprofile", "", "write cpu profile to file (or use CPUPROFILE env var)")
)

var (
	writtenFiles atomic.Int32
	writtenBytes atomic.Int64
	suffix       string
	generator    writer.DataGenerator
)

func main() {
	flag.Parse()

	profilePath := *cpuProfile
	if profilePath == "" {
		profilePath = os.Getenv("CPUPROFILE")
	}
	if profilePath != "" {
		f, err := os.Create(profilePath)
		if err != nil {
			log.Fatalf("Failed to create cpu profile file: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Failed to start cpu profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			if err := f.Close(); err != nil {
				log.Printf("Failed to close cpu profile file: %v", err)
			}
		}()
		log.Printf("CPU profiling enabled: %s", profilePath)
	}

	var cfg config.Config
	toml.DecodeFile(*cfgPath, &cfg)

	chunkCalculator := writer.NewChunkSizeCalculator(&cfg)

	switch strings.ToLower(cfg.Common.FileFormat) {
	case "parquet":
		suffix = "parquet"
		generator = writer.NewParquetGenerator()
	case "csv":
		suffix = "csv"
		generator = writer.NewCSVGenerator(chunkCalculator)
	default:
		log.Fatalf("Unsupported file format: %s", cfg.Common.FileFormat)
	}

	switch strings.ToLower(*operation) {
	case "delete":
		if err := DeleteAllFiles(cfg); err != nil {
			log.Fatalf("Failed to delete files: %v", err)
		}
	case "show":
		if err := ShowFiles(cfg); err != nil {
			log.Fatalf("Failed to show files: %v", err)
		}
	case "create":
		if err := GenerateFiles(cfg); err != nil {
			log.Fatalf("Failed to generate files: %v", err)
		}
	case "upload":
		if *localDir == "" {
			log.Fatalf("Local directory (-dir) must be specified for upload operation")
		}
		if err := UploadLocalFiles(cfg, *localDir); err != nil {
			log.Fatalf("Failed to upload files: %v", err)
		}
	default:
		log.Fatalf("Unknown operation: %s", *operation)
	}
}
