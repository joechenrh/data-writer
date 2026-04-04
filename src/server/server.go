package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"sync"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/generator"
	"dataWriter/src/spec"
	"dataWriter/src/util"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed all:public
var publicFS embed.FS

// DB is the database connection pool, shared across handlers.
var DB *pgxpool.Pool

// Currently running task; protected by runningMu.
var (
	runningMu     sync.Mutex
	runningID     int64
	runningCancel context.CancelFunc
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS tasks (
	id            BIGSERIAL PRIMARY KEY,
	state         TEXT NOT NULL DEFAULT 'pending',
	target        TEXT NOT NULL DEFAULT 'local',
	sql_text      TEXT NOT NULL,
	config_json   JSONB NOT NULL,
	error         TEXT NOT NULL DEFAULT '',
	files_written BIGINT NOT NULL DEFAULT 0,
	total_files   INT NOT NULL DEFAULT 0,
	written_bytes BIGINT NOT NULL DEFAULT 0,
	created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// StartServer initializes the database, starts the background worker, and serves HTTP.
func StartServer(port int, dsn string) {
	var err error
	DB, err = pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer DB.Close()

	if _, err := DB.Exec(context.Background(), createTableSQL); err != nil {
		log.Fatalf("Failed to create tasks table: %v", err)
	}
	log.Println("Database connected, tasks table ready")

	go taskWorker()

	publicContent, err := fs.Sub(publicFS, "public")
	if err != nil {
		log.Fatalf("Failed to get public subdirectory: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServer(http.FS(publicContent)))
	mux.HandleFunc("POST /api/create", handleCreate)
	mux.HandleFunc("GET /api/status", handleStatus)
	mux.HandleFunc("GET /api/tasks", handleListTasks)
	mux.HandleFunc("POST /api/cancel", handleCancel)
	mux.HandleFunc("POST /api/ai-assist", handleAIAssist)

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// taskWorker polls for pending local tasks and runs them one at a time.
func taskWorker() {
	for {
		id, sqlText, cfgJSON, ok := pickPendingTask("local")
		if !ok {
			time.Sleep(2 * time.Second)
			continue
		}
		executeTask(id, sqlText, cfgJSON)
	}
}

// pickPendingTask atomically picks the oldest pending task matching the target and sets it to running.
func pickPendingTask(target string) (id int64, sqlText string, cfgJSON []byte, ok bool) {
	err := DB.QueryRow(context.Background(),
		`UPDATE tasks SET state = 'running', updated_at = now()
		 WHERE id = (SELECT id FROM tasks WHERE state = 'pending' AND target = $1 ORDER BY id LIMIT 1)
		 RETURNING id, sql_text, config_json`, target,
	).Scan(&id, &sqlText, &cfgJSON)
	if err != nil {
		return 0, "", nil, false
	}
	return id, sqlText, cfgJSON, true
}

// executeTask runs a single data generation task to completion.
func executeTask(id int64, sqlText string, cfgJSON []byte) {
	taskCtx, taskCancel := context.WithCancel(context.Background())

	runningMu.Lock()
	runningID = id
	runningCancel = taskCancel
	runningMu.Unlock()

	defer func() {
		taskCancel()
		runningMu.Lock()
		runningID = 0
		runningCancel = nil
		runningMu.Unlock()
	}()

	fail := func(errMsg string) {
		DB.Exec(context.Background(),
			`UPDATE tasks SET state = 'failed', error = $1, updated_at = now() WHERE id = $2`,
			errMsg, id)
	}

	var cfg config.Config
	if err := json.Unmarshal(cfgJSON, &cfg); err != nil {
		fail("invalid config: " + err.Error())
		return
	}

	specs, err := spec.GetSpecFromString(sqlText)
	if err != nil {
		fail("invalid SQL: " + err.Error())
		return
	}

	// Write schema SQL file: $prefix-schema.sql
	if err := writeSchemaFile(&cfg, sqlText); err != nil {
		log.Printf("Warning: failed to write schema file: %v", err)
	}

	util.ResetProgressLogger()

	gen, err := generator.NewOrchestratorFromSpecs(&cfg, specs)
	if err != nil {
		fail(err.Error())
		return
	}
	defer gen.Close()

	// Periodically flush progress to DB.
	flushCtx, flushCancel := context.WithCancel(context.Background())
	defer flushCancel()
	go flushProgress(flushCtx, id)

	if err := gen.RunWithContext(taskCtx, cfg.Common.UseStreamingMode, 16); err != nil {
		if taskCtx.Err() != nil {
			// Cancelled via API — state already set by handleCancel.
			return
		}
		fail(err.Error())
		return
	}

	// Final progress flush.
	if logger := util.GetProgressLogger(); logger != nil {
		files, bytes := logger.Snapshot()
		DB.Exec(context.Background(),
			`UPDATE tasks SET state = 'completed', files_written = $1, written_bytes = $2, updated_at = now() WHERE id = $3`,
			files, bytes, id)
	} else {
		DB.Exec(context.Background(),
			`UPDATE tasks SET state = 'completed', updated_at = now() WHERE id = $1`, id)
	}
}

// writeSchemaFile writes the SQL schema to $prefix-schema.sql in the storage root.
func writeSchemaFile(cfg *config.Config, sqlText string) error {
	store, err := config.GetStore(cfg)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}

	fileName := cfg.Common.Prefix + "-schema.sql"
	ctx := context.Background()
	w, err := store.Create(ctx, fileName, nil)
	if err != nil {
		return fmt.Errorf("create %s: %w", fileName, err)
	}
	_, err = w.Write(ctx, []byte(sqlText))
	if err != nil {
		return fmt.Errorf("write %s: %w", fileName, err)
	}
	return w.Close(ctx)
}

// flushProgress periodically writes live progress to the database.
func flushProgress(ctx context.Context, id int64) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logger := util.GetProgressLogger()
			if logger == nil {
				continue
			}
			files, bytes := logger.Snapshot()
			DB.Exec(context.Background(),
				`UPDATE tasks SET files_written = $1, written_bytes = $2, updated_at = now() WHERE id = $3`,
				files, bytes, id)
		}
	}
}
