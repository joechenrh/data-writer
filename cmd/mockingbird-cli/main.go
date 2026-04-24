package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"dataWriter/src/config"
	"dataWriter/src/spec"

	"github.com/BurntSushi/toml"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	sub := os.Args[1]
	args := os.Args[2:]
	switch sub {
	case "create":
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		sqlPath := fs.String("sql", "", "SQL file path (required)")
		cfgPath := fs.String("cfg", "", "config file path (required)")
		threads := fs.Int("threads", 16, "number of threads")
		cpuProfile := fs.String("cpuprofile", "", "write cpu profile to file (or use CPUPROFILE env var)")
		fs.Parse(args)
		if *sqlPath == "" {
			log.Fatalf("-sql is required")
		}
		if *cfgPath == "" {
			log.Fatalf("-cfg is required")
		}
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
		cfg := loadConfig(*cfgPath)
		if err := GenerateFiles(cfg, *sqlPath, *threads); err != nil {
			log.Fatalf("Failed to generate files: %v", err)
		}
	case "delete":
		fs := flag.NewFlagSet("delete", flag.ExitOnError)
		cfgPath := fs.String("cfg", "", "config file path (required)")
		fs.Parse(args)
		if *cfgPath == "" {
			log.Fatalf("-cfg is required")
		}
		cfg := loadConfig(*cfgPath)
		if err := DeleteAllFiles(cfg); err != nil {
			log.Fatalf("Failed to delete files: %v", err)
		}
	case "show":
		fs := flag.NewFlagSet("show", flag.ExitOnError)
		cfgPath := fs.String("cfg", "", "config file path (required)")
		fs.Parse(args)
		if *cfgPath == "" {
			log.Fatalf("-cfg is required")
		}
		cfg := loadConfig(*cfgPath)
		if err := ShowFiles(cfg); err != nil {
			log.Fatalf("Failed to show files: %v", err)
		}
	case "upload":
		fs := flag.NewFlagSet("upload", flag.ExitOnError)
		cfgPath := fs.String("cfg", "", "config file path (required)")
		localDir := fs.String("dir", "", "local directory (required)")
		threads := fs.Int("threads", 16, "number of threads")
		fs.Parse(args)
		if *cfgPath == "" {
			log.Fatalf("-cfg is required")
		}
		if *localDir == "" {
			log.Fatalf("-dir is required")
		}
		cfg := loadConfig(*cfgPath)
		if err := UploadLocalFiles(cfg, *localDir, *threads); err != nil {
			log.Fatalf("Failed to upload files: %v", err)
		}
	case "download":
		fs := flag.NewFlagSet("download", flag.ExitOnError)
		cfgPath := fs.String("cfg", "", "config file path (required)")
		localDir := fs.String("dir", "", "local directory (required)")
		threads := fs.Int("threads", 16, "number of threads")
		fs.Parse(args)
		if *cfgPath == "" {
			log.Fatalf("-cfg is required")
		}
		if *localDir == "" {
			log.Fatalf("-dir is required")
		}
		cfg := loadConfig(*cfgPath)
		if err := DownloadFiles(cfg, *localDir, *threads); err != nil {
			log.Fatalf("Failed to download files: %v", err)
		}
	case "show-spec":
		fs := flag.NewFlagSet("show-spec", flag.ExitOnError)
		sqlPath := fs.String("sql", "", "SQL file path (required)")
		fs.Parse(args)
		if *sqlPath == "" {
			log.Fatalf("-sql is required")
		}
		specs, err := spec.GetSpecFromSQL(*sqlPath)
		if err != nil {
			log.Fatalf("Failed to parse SQL: %v", err)
		}
		fmt.Print(spec.FormatSpecsTable(specs))
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %q\n\n", sub)
		usage()
		os.Exit(2)
	}
}

func loadConfig(cfgPath string) *config.Config {
	var cfg config.Config
	toml.DecodeFile(cfgPath, &cfg)
	if err := config.Normalize(&cfg); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}
	if err := config.Validate(&cfg); err != nil {
		log.Fatalf("%v", err)
	}
	return &cfg
}

func usage() {
	fmt.Fprintln(os.Stderr, `mockingbird-cli — developer CLI for data generation

Usage:
  mockingbird-cli create     -sql <PATH> -cfg <PATH> [-threads N] [-cpuprofile FILE]
  mockingbird-cli delete     -cfg <PATH>
  mockingbird-cli show       -cfg <PATH>
  mockingbird-cli upload     -cfg <PATH> -dir <PATH> [-threads N]
  mockingbird-cli download   -cfg <PATH> -dir <PATH> [-threads N]
  mockingbird-cli show-spec  -sql <PATH>`)
}
