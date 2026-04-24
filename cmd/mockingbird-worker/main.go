package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"dataWriter/src/server"
	_ "dataWriter/src/user"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	sub := os.Args[1]
	args := os.Args[2:]
	switch sub {
	case "claim":
		fs := flag.NewFlagSet("claim", flag.ExitOnError)
		dsn := fs.String("dsn", "", "database connection string (required)")
		fs.Parse(args)
		requireDSN(*dsn)
		server.ClaimTask(*dsn)
	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		dsn := fs.String("dsn", "", "database connection string (required)")
		taskID := fs.Int64("task-id", 0, "task id (required)")
		shard := fs.Int("shard", 0, "shard index (0-based)")
		shardTotal := fs.Int("shard-total", 1, "total number of shards")
		fs.Parse(args)
		requireDSN(*dsn)
		if *taskID <= 0 {
			log.Fatalf("-task-id is required")
		}
		server.RunWorkerOnce(*dsn, *taskID, *shard, *shardTotal)
	case "dump-generators":
		fs := flag.NewFlagSet("dump-generators", flag.ExitOnError)
		dsn := fs.String("dsn", "", "database connection string (required)")
		taskID := fs.Int64("task-id", 0, "task id (required)")
		fs.Parse(args)
		requireDSN(*dsn)
		if *taskID <= 0 {
			log.Fatalf("-task-id is required")
		}
		if err := server.DumpGenerators(*dsn, *taskID, os.Stdout); err != nil {
			log.Fatalf("dump-generators: %v", err)
		}
	case "report-failure":
		fs := flag.NewFlagSet("report-failure", flag.ExitOnError)
		dsn := fs.String("dsn", "", "database connection string (required)")
		taskID := fs.Int64("task-id", 0, "task id (required)")
		errFile := fs.String("err-file", "", "path to file with error content (required)")
		fs.Parse(args)
		requireDSN(*dsn)
		if *taskID <= 0 {
			log.Fatalf("-task-id is required")
		}
		if *errFile == "" {
			log.Fatalf("-err-file is required")
		}
		if err := server.ReportFailure(*dsn, *taskID, *errFile); err != nil {
			log.Fatalf("report-failure: %v", err)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %q\n\n", sub)
		usage()
		os.Exit(2)
	}
}

func requireDSN(dsn string) {
	if dsn == "" {
		log.Fatalf("-dsn is required")
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `mockingbird-worker — claim and run data-generation tasks on EC2

Usage:
  mockingbird-worker claim            -dsn <DSN>
  mockingbird-worker run              -dsn <DSN> -task-id <N> -shard <N> -shard-total <N>
  mockingbird-worker dump-generators  -dsn <DSN> -task-id <N>
  mockingbird-worker report-failure   -dsn <DSN> -task-id <N> -err-file <PATH>`)
}
