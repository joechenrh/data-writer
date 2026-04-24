package main

import (
	"flag"
	"log"

	"dataWriter/src/server"
	_ "dataWriter/src/user"
)

func main() {
	port := flag.Int("port", 8081, "HTTP server port")
	dsn := flag.String("dsn", "", "database connection string (required)")
	workspace := flag.String("workspace", "", "path to a data-writer source tree (enables /api/validate-generators)")
	flag.Parse()

	if *dsn == "" {
		log.Fatalf("-dsn is required")
	}
	server.StartServer(*port, *dsn, *workspace)
}
