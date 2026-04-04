package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/server"
	"dataWriter/src/spec"

	"github.com/BurntSushi/toml"
)

func main() {
	serve := flag.Bool("serve", false, "start HTTP server mode")
	worker := flag.Bool("worker", false, "run as worker: pick pending tasks from DB and execute")
	checkPending := flag.Bool("check-pending", false, "print number of pending ec2 tasks and exit")
	port := flag.Int("port", 8081, "HTTP server port (only used with -serve)")
	dsn := flag.String("dsn", "", "database connection string (used with -serve or -worker)")
	operation := flag.String("op", "create", "create/delete/show/ls/upload/download, default is create")
	sqlPath := flag.String("sql", "", "sql path")
	cfgPath := flag.String("cfg", "", "config path")
	threads := flag.Int("threads", 16, "threads")
	localDir := flag.String("dir", "", "local directory for upload/download operation")
	cpuProfile := flag.String("cpuprofile", "", "write cpu profile to file (or use CPUPROFILE env var)")
	showSpec := flag.Bool("show-spec", false, "print parsed schema spec and exit")

	flag.Parse()

	if *serve {
		if *dsn == "" {
			log.Fatalf("-dsn is required in server mode")
		}
		server.StartServer(*port, *dsn)
		return
	}

	if *checkPending {
		if *dsn == "" {
			log.Fatalf("-dsn is required")
		}
		server.CheckPending(*dsn)
		return
	}

	if *worker {
		if *dsn == "" {
			log.Fatalf("-dsn is required in worker mode")
		}
		server.RunWorkerLoop(*dsn, 2*time.Minute)
		return
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

	switch strings.ToLower(*operation) {
	case "delete":
		if err := DeleteAllFiles(&cfg); err != nil {
			log.Fatalf("Failed to delete files: %v", err)
		}
	case "show", "ls":
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
	case "download":
		if *localDir == "" {
			log.Fatalf("Local directory (-dir) must be specified for download operation")
		}
		if err := DownloadFiles(&cfg, *localDir, *threads); err != nil {
			log.Fatalf("Failed to download files: %v", err)
		}
	default:
		log.Fatalf("Unknown operation: %s", *operation)
	}
}
