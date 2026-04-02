package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"sync"

	"dataWriter/src/config"
	"dataWriter/src/generator"
	"dataWriter/src/spec"
	"dataWriter/src/util"
)

//go:embed all:public
var publicFS embed.FS

// taskState represents the lifecycle state of a generation task.
type taskState string

const (
	stateIdle      taskState = "idle"
	stateRunning   taskState = "running"
	stateCompleted taskState = "completed"
	stateFailed    taskState = "failed"
)

// taskStatus holds the current task's status, read by the status endpoint.
type taskStatus struct {
	mu    sync.RWMutex
	state taskState
	err   string
	cfg   *config.Config // stored for total_files calculation
}

var currentTask = &taskStatus{state: stateIdle}

// createRequest is the JSON body for POST /api/create.
type createRequest struct {
	SQL         string            `json:"sql"`
	Path        string            `json:"path"`
	Prefix      string            `json:"prefix"`
	StartFileNo int               `json:"start_fileno"`
	EndFileNo   int               `json:"end_fileno"`
	Rows        int               `json:"rows"`
	Format      string            `json:"format"`
	S3          *config.S3Config  `json:"s3,omitempty"`
	GCS         *config.GCSConfig `json:"gcs,omitempty"`
}

func startServer(port int) {
	publicContent, err := fs.Sub(publicFS, "public")
	if err != nil {
		log.Fatalf("Failed to get public subdirectory: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServer(http.FS(publicContent)))
	mux.HandleFunc("POST /api/create", handleCreate)
	mux.HandleFunc("GET /api/status", handleStatus)

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

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

	currentTask.mu.Lock()
	if currentTask.state == stateRunning {
		currentTask.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "task already running"})
		return
	}
	currentTask.state = stateRunning
	currentTask.err = ""
	currentTask.mu.Unlock()

	cfg := buildConfig(req)
	if err := config.Normalize(cfg); err != nil {
		currentTask.mu.Lock()
		currentTask.state = stateFailed
		currentTask.err = err.Error()
		currentTask.mu.Unlock()
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := config.Validate(cfg); err != nil {
		currentTask.mu.Lock()
		currentTask.state = stateFailed
		currentTask.err = err.Error()
		currentTask.mu.Unlock()
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	currentTask.mu.Lock()
	currentTask.cfg = cfg
	currentTask.mu.Unlock()

	// Parse SQL to validate before starting the goroutine.
	specs, err := spec.GetSpecFromString(req.SQL)
	if err != nil {
		currentTask.mu.Lock()
		currentTask.state = stateFailed
		currentTask.err = "invalid SQL: " + err.Error()
		currentTask.mu.Unlock()
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid SQL: " + err.Error()})
		return
	}

	go runTask(cfg, specs)

	writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

func runTask(cfg *config.Config, specs []*spec.ColumnSpec) {
	util.ResetProgressLogger()

	gen, err := generator.NewOrchestratorFromSpecs(cfg, specs)
	if err != nil {
		currentTask.mu.Lock()
		currentTask.state = stateFailed
		currentTask.err = err.Error()
		currentTask.mu.Unlock()
		return
	}
	defer gen.Close()

	if err := gen.Run(cfg.Common.UseStreamingMode, 16); err != nil {
		currentTask.mu.Lock()
		currentTask.state = stateFailed
		currentTask.err = err.Error()
		currentTask.mu.Unlock()
		return
	}

	currentTask.mu.Lock()
	currentTask.state = stateCompleted
	currentTask.mu.Unlock()
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	currentTask.mu.RLock()
	state := currentTask.state
	taskErr := currentTask.err
	cfg := currentTask.cfg
	currentTask.mu.RUnlock()

	resp := map[string]any{
		"state":         string(state),
		"progress":      "0%",
		"speed":         "",
		"files_written": 0,
		"total_files":   0,
		"written_size":  "",
		"error":         taskErr,
	}

	logger := util.GetProgressLogger()
	if logger != nil && state == stateRunning {
		files, bytes := logger.Snapshot()
		totalFiles := 0
		if cfg != nil {
			totalFiles = cfg.Common.EndFileNo - cfg.Common.StartFileNo
		}
		percent := 0
		if totalFiles > 0 {
			percent = int(float64(files) / float64(totalFiles) * 100)
		}
		resp["progress"] = fmt.Sprintf("%d%%", percent)
		resp["files_written"] = files
		resp["total_files"] = totalFiles
		resp["written_size"] = formatBytes(bytes)
	} else if state == stateCompleted && logger != nil {
		files, bytes := logger.Snapshot()
		totalFiles := 0
		if cfg != nil {
			totalFiles = cfg.Common.EndFileNo - cfg.Common.StartFileNo
		}
		resp["progress"] = "100%"
		resp["files_written"] = files
		resp["total_files"] = totalFiles
		resp["written_size"] = formatBytes(bytes)
	}

	writeJSON(w, http.StatusOK, resp)
}

func buildConfig(req createRequest) *config.Config {
	format := strings.ToLower(req.Format)
	if format == "" {
		format = "csv"
	}

	cfg := &config.Config{
		Common: config.CommonConfig{
			Path:             req.Path,
			Prefix:           req.Prefix,
			StartFileNo:      req.StartFileNo,
			EndFileNo:        req.EndFileNo,
			Rows:             req.Rows,
			FileFormat:       format,
			UseStreamingMode: true,
		},
		Parquet: config.ParquetConfig{
			NumRowGroups: 1,
			Compression:  "zstd",
		},
		CSV: config.CSVConfig{
			Separator: ",",
			EndLine:   "\n",
		},
		S3Config:  req.S3,
		GCSConfig: req.GCS,
	}

	if cfg.Common.Prefix == "" {
		cfg.Common.Prefix = "test.t1"
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
