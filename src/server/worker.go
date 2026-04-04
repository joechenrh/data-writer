package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckPending prints the number of pending ec2 tasks and exits.
func CheckPending(dsn string) {
	var err error
	DB, err = pgxpool.New(context.Background(), dsn)
	if err != nil {
		fmt.Println("0")
		return
	}
	defer DB.Close()

	var count int
	err = DB.QueryRow(context.Background(),
		`SELECT count(*) FROM tasks WHERE state = 'pending' AND target = 'ec2'`).Scan(&count)
	if err != nil {
		fmt.Println("0")
		return
	}
	fmt.Println(count)
}

// RunWorkerLoop connects to the database and continuously picks pending tasks.
// If idleTimeout > 0, exits after that duration of inactivity.
// If idleTimeout == 0, runs forever.
func RunWorkerLoop(dsn string, idleTimeout time.Duration) {
	var err error
	DB, err = pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer DB.Close()

	idleStart := time.Now()
	for {
		id, sqlText, cfgJSON, ok := pickPendingTask("ec2")
		if !ok {
			if idleTimeout > 0 && time.Since(idleStart) > idleTimeout {
				log.Printf("No tasks for %s, exiting", idleTimeout)
				return
			}
			time.Sleep(5 * time.Second)
			continue
		}
		idleStart = time.Now()
		log.Printf("Picked task %d, executing...", id)
		executeTask(id, sqlText, cfgJSON)
	}
}
