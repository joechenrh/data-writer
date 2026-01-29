package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"

	"dataWriter/src/config"
	"dataWriter/src/spec"

	"github.com/BurntSushi/toml"
)

func main() {
	operation := flag.String("op", "create", "create/delete/show/upload, default is create")
	sqlPath := flag.String("sql", "", "sql path")
	cfgPath := flag.String("cfg", "", "config path")
	threads := flag.Int("threads", 16, "threads")
	localDir := flag.String("dir", "", "local directory for upload operation")
	cpuProfile := flag.String("cpuprofile", "", "write cpu profile to file (or use CPUPROFILE env var)")
	showSpec := flag.Bool("show-spec", false, "print parsed schema spec and exit")

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
	if err := config.Normalize(&cfg); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}
	if err := config.Validate(&cfg); err != nil {
		log.Fatalf("%v", err)
	}

	if *showSpec {
		if *sqlPath == "" {
			log.Fatalf("SQL file (-sql) is required for -show-spec")
		}
		specs, err := spec.GetSpecFromSQL(*sqlPath)
		if err != nil {
			log.Fatalf("Failed to parse SQL: %v", err)
		}
		fmt.Print(spec.FormatSpecsTable(specs))
		return
	}

	switch strings.ToLower(*operation) {
	case "delete":
		if err := DeleteAllFiles(&cfg); err != nil {
			log.Fatalf("Failed to delete files: %v", err)
		}
	case "show":
		if err := ShowFiles(&cfg); err != nil {
			log.Fatalf("Failed to show files: %v", err)
		}
	case "create":
		if err := GenerateFiles(&cfg, *sqlPath, *threads); err != nil {
			log.Fatalf("Failed to generate files: %v", err)
		}
	case "upload":
		if *localDir == "" {
			log.Fatalf("Local directory (-dir) must be specified for upload operation")
		}
		if err := UploadLocalFiles(&cfg, *localDir, *threads); err != nil {
			log.Fatalf("Failed to upload files: %v", err)
		}
	default:
		log.Fatalf("Unknown operation: %s", *operation)
	}
}
