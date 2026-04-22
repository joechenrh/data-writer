package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"

	"dataWriter/src/config"
	"dataWriter/src/server"
	"dataWriter/src/spec"

	"github.com/BurntSushi/toml"
)

func main() {
	serve := flag.Bool("serve", false, "start HTTP server mode")
	worker := flag.Bool("worker", false, "run as worker: execute one shard of an ec2 task")
	workerTaskID := flag.Int64("task-id", 0, "task id to run (worker mode)")
	workerShard := flag.Int("shard", 0, "shard index to run (worker mode, 0-based)")
	workerShardTotal := flag.Int("shard-total", 1, "total number of shards for the task (worker mode)")
	claimTask := flag.Bool("claim-task", false, "atomically reserve one shard of work and print '<id> <shard> <total>' (or '0' if none)")
	dumpGens := flag.Bool("dump-generators", false, "dump tasks.generators_go for -task-id to stdout")
	reportFailure := flag.Bool("report-failure", false, "set tasks.state=failed with error content from -err-file")
	errFile := flag.String("err-file", "", "path to file with error content (used with -report-failure)")
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

	if *claimTask {
		if *dsn == "" {
			log.Fatalf("-dsn is required")
		}
		server.ClaimTask(*dsn)
		return
	}

	if *dumpGens {
		if *dsn == "" {
			log.Fatalf("-dsn is required")
		}
		if *workerTaskID <= 0 {
			log.Fatalf("-task-id is required")
		}
		if err := server.DumpGenerators(*dsn, *workerTaskID, os.Stdout); err != nil {
			log.Fatalf("dump-generators: %v", err)
		}
		return
	}

	if *reportFailure {
		if *dsn == "" {
			log.Fatalf("-dsn is required")
		}
		if *workerTaskID <= 0 {
			log.Fatalf("-task-id is required")
		}
		if *errFile == "" {
			log.Fatalf("-err-file is required")
		}
		if err := server.ReportFailure(*dsn, *workerTaskID, *errFile); err != nil {
			log.Fatalf("report-failure: %v", err)
		}
		return
	}

	if *worker {
		if *dsn == "" {
			log.Fatalf("-dsn is required in worker mode")
		}
		if *workerTaskID <= 0 {
			log.Fatalf("-task-id is required in worker mode")
		}
		server.RunWorkerOnce(*dsn, *workerTaskID, *workerShard, *workerShardTotal)
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
