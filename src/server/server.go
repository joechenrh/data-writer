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
	id             BIGSERIAL PRIMARY KEY,
	state          TEXT NOT NULL DEFAULT 'pending',
	target         TEXT NOT NULL DEFAULT 'local',
	sql_text       TEXT NOT NULL,
	config_json    JSONB NOT NULL,
	error          TEXT NOT NULL DEFAULT '',
	files_written  BIGINT NOT NULL DEFAULT 0,
	total_files    INT NOT NULL DEFAULT 0,
	written_bytes  BIGINT NOT NULL DEFAULT 0,
	worker_total   INT NOT NULL DEFAULT 1,
	worker_claimed INT NOT NULL DEFAULT 0,
	worker_done    INT NOT NULL DEFAULT 0,
	generators_go  TEXT,
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// migrateTableSQL adds new columns to pre-existing task tables.
const migrateTableSQL = `
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS worker_total   INT NOT NULL DEFAULT 1;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS worker_claimed INT NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS worker_done    INT NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS generators_go  TEXT;
`

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
	if _, err := DB.Exec(context.Background(), migrateTableSQL); err != nil {
		log.Fatalf("Failed to migrate tasks table: %v", err)
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
	mux.HandleFunc("POST /api/scaffold", handleScaffold)
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

// pickLaunchingTask picks a task in "launching" state (claimed by launcher, waiting for worker).
func pickLaunchingTask() (id int64, sqlText string, cfgJSON []byte, ok bool) {
	err := DB.QueryRow(context.Background(),
		`UPDATE tasks SET state = 'running', updated_at = now()
		 WHERE id = (SELECT id FROM tasks WHERE state = 'launching' ORDER BY id LIMIT 1)
		 RETURNING id, sql_text, config_json`,
	).Scan(&id, &sqlText, &cfgJSON)
	if err != nil {
		return 0, "", nil, false
	}
	return id, sqlText, cfgJSON, true
}

// executeTask runs a single-shard, locally-tracked data generation task to
// completion. Used by the local task worker; equivalent to a 1-of-1 shard.
func executeTask(id int64, sqlText string, cfgJSON []byte) {
	executeShardImpl(id, sqlText, cfgJSON, 0, 1, true /*tracksRunning*/)
}

// executeShard runs one shard of an EC2 task. Multiple shards may run
// concurrently across instances and contribute to the same task row.
func executeShard(id int64, sqlText string, cfgJSON []byte, shard, shardTotal int) {
	executeShardImpl(id, sqlText, cfgJSON, shard, shardTotal, false)
}

// executeShardImpl is the shared implementation. tracksRunning controls whether
// we publish the local in-memory cancel handle (only the in-process local task
// worker should set this).
func executeShardImpl(id int64, sqlText string, cfgJSON []byte, shard, shardTotal int, tracksRunning bool) {
	taskCtx, taskCancel := context.WithCancel(context.Background())

	if tracksRunning {
		runningMu.Lock()
		runningID = id
		runningCancel = taskCancel
		runningMu.Unlock()
		defer func() {
			runningMu.Lock()
			runningID = 0
			runningCancel = nil
			runningMu.Unlock()
		}()
	}
	defer taskCancel()

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

	// Narrow this shard's file range. shardRange is in worker.go.
	if shardTotal > 1 {
		lo, hi := shardRange(cfg.Common.StartFileNo, cfg.Common.EndFileNo, shard, shardTotal)
		cfg.Common.StartFileNo = lo
		cfg.Common.EndFileNo = hi
		log.Printf("Task %d shard %d/%d covers files [%d,%d)", id, shard, shardTotal, lo, hi)
	}

	// Only the first shard writes the schema sidecar file.
	if shard == 0 {
		if err := writeSchemaFile(&cfg, sqlText); err != nil {
			log.Printf("Warning: failed to write schema file: %v", err)
		}
	}

	util.ResetProgressLogger()

	gen, err := generator.NewOrchestratorFromSpecs(&cfg, specs)
	if err != nil {
		fail(err.Error())
		return
	}
	defer gen.Close()

	// Periodically flush progress (delta-based, safe for concurrent shards).
	flushCtx, flushCancel := context.WithCancel(context.Background())
	defer flushCancel()
	go flushProgress(flushCtx, id)

	// Watchdog: poll task state in DB and cancel local context if it flips to
	// 'failed' (e.g. via handleCancel from another process).
	go watchTaskCancel(flushCtx, id, taskCancel)

	if err := gen.RunWithContext(taskCtx, cfg.Common.UseStreamingMode, 16); err != nil {
		if taskCtx.Err() != nil {
			// Cancelled — state already set by handleCancel or watchdog.
			return
		}
		fail(err.Error())
		return
	}

	// Final delta flush so the DB matches what this shard actually produced.
	flushOnce(id)

	// Mark this shard done; if it was the last one, transition to completed.
	finalizeShard(id)
}

// finalizeShard atomically increments worker_done. The worker that drives the
// counter to worker_total transitions the task state to 'completed'.
func finalizeShard(id int64) {
	var done, total int
	err := DB.QueryRow(context.Background(),
		`UPDATE tasks SET worker_done = worker_done + 1, updated_at = now()
		 WHERE id = $1 RETURNING worker_done, worker_total`, id,
	).Scan(&done, &total)
	if err != nil {
		log.Printf("finalizeShard: %v", err)
		return
	}
	if done >= total {
		DB.Exec(context.Background(),
			`UPDATE tasks SET state = 'completed', updated_at = now()
			 WHERE id = $1 AND state = 'running'`, id)
	}
}

// watchTaskCancel polls the task state every 5s; if the task is no longer
// running (e.g. handleCancel set it to 'failed'), it cancels the local context.
func watchTaskCancel(ctx context.Context, id int64, cancel context.CancelFunc) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var state string
			err := DB.QueryRow(context.Background(),
				`SELECT state FROM tasks WHERE id = $1`, id).Scan(&state)
			if err != nil {
				continue
			}
			if state != "running" && state != "launching" {
				log.Printf("Task %d state=%q, cancelling local execution", id, state)
				cancel()
				return
			}
		}
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

// progressTracker holds the per-shard delta state used by flushProgress so
// concurrent shards write additive updates instead of overwriting each other.
type progressTracker struct {
	lastFiles int64
	lastBytes int64
}

// flushDelta computes the diff since the previous flush and atomically adds it
// to the task row. Safe to call concurrently from multiple shards.
func (p *progressTracker) flushDelta(id int64) {
	logger := util.GetProgressLogger()
	if logger == nil {
		return
	}
	files, bytes := logger.Snapshot()
	dFiles := files - p.lastFiles
	dBytes := bytes - p.lastBytes
	if dFiles == 0 && dBytes == 0 {
		return
	}
	if _, err := DB.Exec(context.Background(),
		`UPDATE tasks SET files_written = files_written + $1, written_bytes = written_bytes + $2, updated_at = now() WHERE id = $3`,
		dFiles, dBytes, id); err != nil {
		return
	}
	p.lastFiles = files
	p.lastBytes = bytes
}

// flushProgress periodically pushes per-shard progress deltas to the DB.
func flushProgress(ctx context.Context, id int64) {
	tracker := &progressTracker{}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tracker.flushDelta(id)
		}
	}
}

// flushOnce drains any remaining progress delta into the DB. Used at shard
// completion so the final values reflect everything this shard produced.
func flushOnce(id int64) {
	tracker := &progressTracker{}
	tracker.flushDelta(id)
}
