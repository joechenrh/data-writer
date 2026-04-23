package server

import (
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"strings"
	"time"

	"dataWriter/src/config"
	"dataWriter/src/spec"
	"dataWriter/src/util"
)

// createRequest is the JSON body for POST /api/create.
type createRequest struct {
	SQL          string                `json:"sql"`
	GeneratorsGo string                `json:"generators_go,omitempty"`
	Path         string                `json:"path"`
	Folders     *int                  `json:"folders,omitempty"`
	StartFileNo int                   `json:"start_fileno"`
	EndFileNo   int                   `json:"end_fileno"`
	Rows        int                   `json:"rows"`
	Format      string                `json:"format"`
	Target      string                `json:"target,omitempty"`
	Ksyun       bool                  `json:"ksyun,omitempty"`
	CSV         *config.CSVConfig     `json:"csv,omitempty"`
	Parquet     *config.ParquetConfig `json:"parquet,omitempty"`
	S3          *config.S3Config      `json:"s3,omitempty"`
	GCS         *config.GCSConfig     `json:"gcs,omitempty"`
}

// handleCreate validates the request, inserts a pending task, and returns its ID.
func handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sql is required"})
		return
	}
	if req.Path == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path is required"})
		return
	}

	// Derive prefix (schema.table) from SQL.
	prefix, err := spec.GetSchemaTableNameFromSQL(req.SQL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "SQL must be CREATE TABLE schema.table (...): " + err.Error()})
		return
	}

	if _, err := spec.GetSpecFromString(req.SQL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid SQL: " + err.Error()})
		return
	}

	cfg := buildConfig(req, prefix)
	if err := config.Normalize(cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := config.Validate(cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to marshal config"})
		return
	}

	totalFiles := cfg.Common.EndFileNo - cfg.Common.StartFileNo

	target := req.Target
	if target != "ec2" {
		target = "local"
	}

	if req.GeneratorsGo != "" {
		if target != "ec2" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "generators_go requires target=ec2"})
			return
		}
		if err := validateGeneratorsGo(req.GeneratorsGo); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "generators_go parse error: " + err.Error()})
			return
		}
		if err := checkRowsPerRowGroup(cfg.Common.Rows, cfg.Parquet.NumRowGroups); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}

	var id int64
	err = DB.QueryRow(r.Context(),
		`INSERT INTO tasks (sql_text, config_json, total_files, target, generators_go)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''))
		 RETURNING id`,
		req.SQL, cfgJSON, totalFiles, target, req.GeneratorsGo,
	).Scan(&id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create task: " + err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"task_id": id})
}

// handleStatus returns the status of a single task by ID.
func handleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}

	var state, taskErr string
	var filesWritten int64
	var totalFiles int
	var writtenBytes int64
	var createdAt, updatedAt time.Time

	err := DB.QueryRow(r.Context(),
		`SELECT state, error, files_written, total_files, written_bytes, created_at, updated_at
		 FROM tasks WHERE id = $1`, id,
	).Scan(&state, &taskErr, &filesWritten, &totalFiles, &writtenBytes, &createdAt, &updatedAt)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

	// For the actively running task, read live progress from in-memory logger.
	runningMu.Lock()
	isActive := fmt.Sprintf("%d", runningID) == id
	runningMu.Unlock()

	if state == "running" && isActive {
		if logger := util.GetProgressLogger(); logger != nil {
			filesWritten, writtenBytes = logger.Snapshot()
		}
	}

	percent := 0
	if totalFiles > 0 {
		percent = int(float64(filesWritten) / float64(totalFiles) * 100)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":            id,
		"state":         state,
		"progress":      fmt.Sprintf("%d%%", percent),
		"files_written": filesWritten,
		"total_files":   totalFiles,
		"written_size":  formatBytes(writtenBytes),
		"error":         taskErr,
		"created_at":    createdAt.Format(time.RFC3339),
		"updated_at":    updatedAt.Format(time.RFC3339),
	})
}

// handleListTasks returns all tasks ordered by newest first.
func handleListTasks(w http.ResponseWriter, r *http.Request) {
	rows, err := DB.Query(r.Context(),
		`SELECT id, state, target, error, files_written, total_files, written_bytes, created_at, updated_at
		 FROM tasks ORDER BY id DESC LIMIT 10`)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed: " + err.Error()})
		return
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var id int64
		var state, target, taskErr string
		var filesWritten int64
		var totalFiles int
		var writtenBytes int64
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&id, &state, &target, &taskErr, &filesWritten, &totalFiles, &writtenBytes, &createdAt, &updatedAt); err != nil {
			continue
		}

		percent := 0
		if totalFiles > 0 {
			percent = int(float64(filesWritten) / float64(totalFiles) * 100)
		}

		result = append(result, map[string]any{
			"id":            id,
			"state":         state,
			"target":        target,
			"progress":      fmt.Sprintf("%d%%", percent),
			"files_written": filesWritten,
			"total_files":   totalFiles,
			"written_size":  formatBytes(writtenBytes),
			"error":         taskErr,
			"created_at":    createdAt.Format(time.RFC3339),
			"updated_at":    updatedAt.Format(time.RFC3339),
		})
	}

	if result == nil {
		result = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCancel cancels a pending or running task.
func handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}

	// Mark task as cancelled in DB. Covers pending, launching (claimed by an
	// EC2 launcher but no worker yet), and running (in-process local OR on an
	// EC2 worker) states. EC2 workers poll task state and exit when they see
	// 'failed'.
	tag, err := DB.Exec(r.Context(),
		`UPDATE tasks SET state = 'failed', error = 'cancelled', updated_at = now()
		 WHERE id = $1 AND state IN ('pending', 'launching', 'running')`, id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "db error: " + err.Error()})
		return
	}
	if tag.RowsAffected() == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task is not pending, launching, or running"})
		return
	}

	// If the task is running locally on this server, cancel its context too so
	// the in-process generator stops immediately (instead of only on next poll).
	runningMu.Lock()
	isActive := fmt.Sprintf("%d", runningID) == id
	cancel := runningCancel
	runningMu.Unlock()
	if isActive && cancel != nil {
		cancel()
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func buildConfig(req createRequest, prefix string) *config.Config {
	format := strings.ToLower(req.Format)
	if format == "" {
		format = "csv"
	}

	folders := 0
	if req.Folders != nil {
		folders = *req.Folders
	}

	cfg := &config.Config{
		Common: config.CommonConfig{
			Path:             req.Path,
			Prefix:           prefix,
			Folders:          folders,
			StartFileNo:      req.StartFileNo,
			EndFileNo:        req.EndFileNo,
			Rows:             req.Rows,
			FileFormat:       format,
			UseStreamingMode: true,
		},
		Parquet: func() config.ParquetConfig {
			if req.Parquet != nil {
				p := *req.Parquet
				if p.NumRowGroups == 0 {
					p.NumRowGroups = 1
				}
				if p.Compression == "" {
					p.Compression = "zstd"
				}
				return p
			}
			return config.ParquetConfig{NumRowGroups: 1, Compression: "zstd"}
		}(),
		CSV: func() config.CSVConfig {
			if req.CSV != nil {
				c := *req.CSV
				if c.Separator == "" {
					c.Separator = ","
				}
				if c.EndLine == "" {
					c.EndLine = "\n"
				}
				return c
			}
			return config.CSVConfig{Separator: ",", EndLine: "\n"}
		}(),
		S3Config:  req.S3,
		GCSConfig: req.GCS,
	}

	// For Ksyun, append KSYUN_KEY query params to the path.
	if req.Ksyun {
		ksyunKey := os.Getenv("KSYUN_KEY")
		if ksyunKey != "" {
			sep := "?"
			if strings.Contains(cfg.Common.Path, "?") {
				sep = "&"
			}
			cfg.Common.Path = cfg.Common.Path + sep + ksyunKey
		}
	}

	if cfg.Common.Rows == 0 {
		cfg.Common.Rows = 60000
	}

	return cfg
}

func formatBytes(b int64) string {
	const (
		kib = 1024
		mib = kib * 1024
		gib = mib * 1024
	)
	switch {
	case b >= gib:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(gib))
	case b >= mib:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(mib))
	case b >= kib:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(kib))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func validateGeneratorsGo(src string) error {
	fset := token.NewFileSet()
	_, err := parser.ParseFile(fset, "generators_go", src, parser.SkipObjectResolution)
	return err
}

func checkRowsPerRowGroup(rows, rowGroups int) error {
	if rowGroups <= 0 {
		return fmt.Errorf("row_groups must be > 0")
	}
	if rows/rowGroups > 2_000_000 {
		return fmt.Errorf("with custom generators, rows / row_groups must be <= 2_000_000 (got %d rows / %d groups = %d)",
			rows, rowGroups, rows/rowGroups)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
