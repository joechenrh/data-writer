package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DumpGenerators writes tasks.generators_go for the given task ID to w.
// If the column is NULL, writes nothing. Returns an error on DB failure.
func DumpGenerators(dsn string, taskID int64, w io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	var gens *string
	err = pool.QueryRow(ctx, `SELECT generators_go FROM tasks WHERE id = $1`, taskID).Scan(&gens)
	if err != nil {
		return fmt.Errorf("query task %d: %w", taskID, err)
	}
	if gens != nil {
		if _, err := io.WriteString(w, *gens); err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}
	return nil
}

// ReportFailure reads up to 64 KB from errFile and sets
// tasks.state='failed', tasks.error=<content> for the given task ID.
func ReportFailure(dsn string, taskID int64, errFile string) error {
	content, err := readBoundedFile(errFile, 64*1024)
	if err != nil {
		return fmt.Errorf("read err-file %s: %w", errFile, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	_, err = pool.Exec(ctx,
		`UPDATE tasks SET state='failed', error=$1, updated_at=now() WHERE id=$2`,
		string(content), taskID)
	if err != nil {
		return fmt.Errorf("update task %d: %w", taskID, err)
	}
	return nil
}

func readBoundedFile(path string, maxBytes int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, maxBytes)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	return buf[:n], nil
}
