package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/spec"
	"dataWriter/src/util"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sharding parameters: spawn one worker per 5 TiB of estimated output, capped at 8.
const (
	bytesPerShard = int64(5) * 1024 * 1024 * 1024 * 1024 // 5 TiB
	maxShards     = 8
)

// ClaimTask atomically reserves one shard of work for an EC2 worker and prints
// "<task_id> <shard_idx> <shard_total>" on success, or "0" if no work is available.
//
// Two cases:
//  1. There is already a 'launching' task with un-claimed shards: increment its
//     worker_claimed counter and return the next shard index.
//  2. There is a 'pending' ec2 task: estimate its output size, decide how many
//     shards to spawn, transition it to 'launching' with worker_total=N and
//     worker_claimed=1, and return shard 0.
//
// The launcher script calls this in a loop, spawning one EC2 worker per claim.
func ClaimTask(dsn string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Println("0")
		return
	}
	defer pool.Close()
	DB = pool

	id, shard, total, ok := claimNextShard(ctx)
	if !ok {
		fmt.Println("0")
		return
	}
	fmt.Printf("%d %d %d\n", id, shard, total)
}

// claimNextShard atomically reserves one shard slot from any task. It first
// looks for an existing 'launching' task with capacity, then falls back to
// initializing a new pending task.
func claimNextShard(ctx context.Context) (id int64, shard int, total int, ok bool) {
	// Case 1: claim a shard from an existing launching task.
	err := DB.QueryRow(ctx, `
		UPDATE tasks
		SET worker_claimed = worker_claimed + 1, updated_at = now()
		WHERE id = (
			SELECT id FROM tasks
			WHERE state = 'launching'
			  AND target = 'ec2'
			  AND worker_claimed < worker_total
			ORDER BY id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, worker_claimed - 1, worker_total
	`).Scan(&id, &shard, &total)
	if err == nil {
		return id, shard, total, true
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Printf("ClaimTask: launching-shard query failed: %v", err)
	}

	// Case 2: initialize a new pending task. Use a transaction so the SELECT
	// FOR UPDATE lock is held across the subsequent UPDATE.
	tx, err := DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		log.Printf("ClaimTask: begin tx failed: %v", err)
		return 0, 0, 0, false
	}
	defer tx.Rollback(ctx)

	var sqlText string
	var cfgJSON []byte
	err = tx.QueryRow(ctx, `
		SELECT id, sql_text, config_json FROM tasks
		WHERE state = 'pending' AND target = 'ec2'
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&id, &sqlText, &cfgJSON)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Printf("ClaimTask: pending query failed: %v", err)
		}
		return 0, 0, 0, false
	}

	n := decideShardCount(sqlText, cfgJSON)
	_, err = tx.Exec(ctx, `
		UPDATE tasks
		SET state = 'launching', worker_total = $1, worker_claimed = 1, updated_at = now()
		WHERE id = $2
	`, n, id)
	if err != nil {
		log.Printf("ClaimTask: launch update failed: %v", err)
		return 0, 0, 0, false
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("ClaimTask: commit failed: %v", err)
		return 0, 0, 0, false
	}
	return id, 0, n, true
}

// decideShardCount estimates the total output size for a task and picks the
// number of EC2 workers to spawn. Defaults to 1 worker on any error.
func decideShardCount(sqlText string, cfgJSON []byte) int {
	var cfg config.Config
	if err := json.Unmarshal(cfgJSON, &cfg); err != nil {
		log.Printf("decideShardCount: invalid config_json: %v", err)
		return 1
	}
	specs, err := spec.GetSpecFromString(sqlText)
	if err != nil {
		log.Printf("decideShardCount: invalid sql_text: %v", err)
		return 1
	}
	rowSize := int64(util.NewChunkSizeCalculator(&cfg).EstimateRowSize(specs))
	if rowSize <= 0 {
		return 1
	}
	files := int64(cfg.Common.EndFileNo - cfg.Common.StartFileNo)
	rows := int64(cfg.Common.Rows)
	if files <= 0 || rows <= 0 {
		return 1
	}
	totalBytes := rowSize * files * rows

	// floor(totalBytes / 5TiB), clamped to [1, 8] and not exceeding the file count.
	n := min(maxShards, max(1, int(totalBytes/bytesPerShard)))
	if int64(n) > files {
		n = int(files)
	}
	log.Printf("decideShardCount: %d files * %d rows * %d row-bytes = %d bytes -> %d shards",
		files, rows, rowSize, totalBytes, n)
	return n
}

// shardRange returns the half-open file range [lo, hi) assigned to a shard.
// Files are split as evenly as possible across shards; any remainder is
// distributed across the first `rem` shards.
func shardRange(start, end, shard, total int) (int, int) {
	if total < 1 {
		total = 1
	}
	if shard < 0 {
		shard = 0
	}
	if shard >= total {
		shard = total - 1
	}
	span := end - start
	chunk := span / total
	rem := span % total
	lo := start + shard*chunk
	if shard < rem {
		lo += shard
	} else {
		lo += rem
	}
	hi := lo + chunk
	if shard < rem {
		hi++
	}
	return lo, hi
}

// RunWorkerOnce runs a single shard of a task on an EC2 worker instance.
// taskID, shard, and shardTotal must come from a prior ClaimTask call (passed
// via the launcher's user-data script). On success it transitions the task to
// 'completed' iff this is the last shard to finish.
func RunWorkerOnce(dsn string, taskID int64, shard, shardTotal int) {
	if taskID <= 0 {
		log.Fatalf("RunWorkerOnce: -task-id is required")
	}
	if shardTotal < 1 {
		shardTotal = 1
	}
	if shard < 0 || shard >= shardTotal {
		log.Fatalf("RunWorkerOnce: invalid shard %d of %d", shard, shardTotal)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()
	DB = pool

	sqlText, cfgJSON, ok := loadTaskForShard(ctx, taskID)
	if !ok {
		log.Printf("Task %d not in launching/running state, skipping", taskID)
		return
	}

	log.Printf("Task %d shard %d/%d: starting", taskID, shard, shardTotal)
	executeShard(taskID, sqlText, cfgJSON, shard, shardTotal)
	log.Printf("Task %d shard %d/%d: done", taskID, shard, shardTotal)
}

// loadTaskForShard fetches the task SQL/config and ensures the task is in a
// state that workers can run. The first worker to arrive flips the state from
// 'launching' to 'running'; subsequent workers see 'running' and proceed.
func loadTaskForShard(ctx context.Context, taskID int64) (sqlText string, cfgJSON []byte, ok bool) {
	var state string
	err := DB.QueryRow(ctx,
		`UPDATE tasks SET state = 'running', updated_at = now()
		 WHERE id = $1 AND state IN ('launching', 'running')
		 RETURNING sql_text, config_json, state`,
		taskID,
	).Scan(&sqlText, &cfgJSON, &state)
	if err != nil {
		log.Printf("loadTaskForShard: %v", err)
		return "", nil, false
	}
	return sqlText, cfgJSON, true
}
