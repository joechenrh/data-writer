# data-writer Web Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an HTTP server mode to data-writer with a browser dashboard for triggering data generation tasks.

**Architecture:** A `-serve` flag in main.go branches into an HTTP server (Go `net/http`). The server exposes 3 endpoints: static frontend (`GET /`), create task (`POST /api/create`), and status polling (`GET /api/status`). Frontend is embedded via `//go:embed`. A single in-memory task slot handles one generation at a time.

**Tech Stack:** Go stdlib `net/http`, `encoding/json`, `embed`; vanilla HTML/CSS/JS frontend.

---

## File Structure

```
src/
  main.go              # MODIFY: add -serve/-port flags, branch to server mode
  server.go            # CREATE: HTTP server, handlers, task runner, embed directive
  operations.go        # UNCHANGED
  spec/
    spec.go            # MODIFY: extract GetSpecFromString() from GetSpecFromSQL()
  util/
    progress.go        # MODIFY: add ResetProgressLogger() to support repeated runs
public/                # CREATE: new directory
  index.html           # CREATE: dashboard page
  app.js               # CREATE: form logic, status polling
  style.css            # CREATE: styling
```

---

### Task 1: Add `GetSpecFromString` to spec package

The server receives SQL as a string in the JSON body, but `GetSpecFromSQL` reads from a file. Extract the core logic into a string-based function.

**Files:**
- Modify: `src/spec/spec.go:420-450`

- [ ] **Step 1: Refactor GetSpecFromSQL to use a new GetSpecFromString**

In `src/spec/spec.go`, add a new exported function `GetSpecFromString` that takes a SQL string directly. Then refactor `GetSpecFromSQL` to call it.

The refactoring approach: move the entire body of `GetSpecFromSQL` (lines 426-498) into a new `GetSpecFromString` function, then have `GetSpecFromSQL` call it. The key change is that `GetSpecFromString` takes the raw SQL string, while `GetSpecFromSQL` reads from a file path first.

```go
// GetSpecFromString parses a CREATE TABLE SQL string and returns column specs.
func GetSpecFromString(query string) ([]*ColumnSpec, error) {
	tbInfo, err := getTableInfoBySQL(query)
	if err != nil {
		return nil, err
	}

	specs := make([]*ColumnSpec, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		spec, ok := DefaultSpecs[col.GetType()]
		if !ok {
			return nil, errors.New("unsupported column type: " + strconv.Itoa(int(col.GetType())))
		}
		spec = spec.Clone()
		spec.OrigName = col.Name.L
		spec.Order = NumericRandomOrder
		spec.Compress = 100 // default no compression for data generation

		if !types.IsTypeNumeric(col.GetType()) && col.GetFlen() > 0 {
			spec.TypeLen = min(col.GetFlen(), 64)
		}
		if col.GetType() == mysql.TypeNewDecimal {
			spec.Precision = col.FieldType.GetFlen()
			spec.Scale = col.FieldType.GetDecimal()
			if spec.Precision == 0 {
				return nil, errors.New("unsupported decimal precision=0 for column: " + spec.OrigName)
			}
			if spec.Scale < 0 || spec.Scale > spec.Precision {
				return nil, errors.New("invalid decimal scale for column: " + spec.OrigName)
			}
			spec.Type, spec.TypeLen = deduceTypeForDecimal(spec.Precision)
		}
		if col.Comment != "" {
			if err := spec.parseComment(col.Comment); err != nil {
				return nil, err
			}
		}

		if spec.MinLen == 0 {
			spec.MinLen = int(float64(spec.TypeLen) * 0.75)
		}
		spec.MinLen = min(spec.TypeLen, spec.MinLen)

		specs = append(specs, spec)
	}

	for _, index := range tbInfo.Indices {
		if index.Primary && len(index.Columns) > 1 {
			return nil, errors.New("multi-column primary key is unsupported")
		}
	}

	if tbInfo.PKIsHandle {
		for _, col := range tbInfo.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				specs[col.Offset].IsUnique = true
				break
			}
		}
	}

	for _, index := range tbInfo.Indices {
		if !index.Primary && !index.Unique {
			continue
		}
		if len(index.Columns) != 1 {
			continue
		}
		col := index.Columns[0]
		if col.Offset < len(specs) && col.Offset >= 0 {
			specs[col.Offset].IsUnique = true
		}
	}

	return specs, nil
}

// GetSpecFromSQL reads a SQL file and returns column specs.
func GetSpecFromSQL(sqlPath string) ([]*ColumnSpec, error) {
	query, err := readAndCleanSQL(sqlPath)
	if err != nil {
		return nil, err
	}
	return GetSpecFromString(query)
}
```

This replaces the existing `GetSpecFromSQL` body (lines 420-499) with a call to `GetSpecFromString`, keeping all existing behavior identical.

- [ ] **Step 2: Verify build**

Run: `cd /mnt/data/joechenrh/data-writer && make build`
Expected: Compiles successfully.

- [ ] **Step 3: Commit**

```bash
git add src/spec/spec.go
git commit -m "refactor: extract GetSpecFromString from GetSpecFromSQL"
```

---

### Task 2: Add `ResetProgressLogger` to util package

The global singleton `ProgressLogger` is only created once (`if globalProgressLogger == nil`). In server mode, each new task needs a fresh logger. Add a reset function.

**Files:**
- Modify: `src/util/progress.go:42-63`

- [ ] **Step 1: Add ResetProgressLogger function**

In `src/util/progress.go`, add after the `GetProgressLogger` function:

```go
// ResetProgressLogger clears the global progress logger so a new one can be created.
func ResetProgressLogger() {
	globalProgressLogger = nil
}
```

- [ ] **Step 2: Verify build**

Run: `cd /mnt/data/joechenrh/data-writer && make build`
Expected: Compiles successfully.

- [ ] **Step 3: Commit**

```bash
git add src/util/progress.go
git commit -m "feat: add ResetProgressLogger for repeated runs"
```

---

### Task 3: Create the HTTP server (`server.go`)

This is the core of the web service. It handles request routing, task lifecycle, and progress reporting.

**Files:**
- Create: `src/server.go`

- [ ] **Step 1: Create server.go with all handler logic**

Create `src/server.go` with the following content:

```go
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
	SQL        string            `json:"sql"`
	Path       string            `json:"path"`
	Prefix     string            `json:"prefix"`
	StartFileNo int             `json:"start_fileno"`
	EndFileNo   int             `json:"end_fileno"`
	Rows        int             `json:"rows"`
	Format      string          `json:"format"`
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
```

- [ ] **Step 2: Verify the file compiles (will fail until Task 4 adds NewOrchestratorFromSpecs)**

This step is just for awareness — `server.go` references `generator.NewOrchestratorFromSpecs` which doesn't exist yet. It will compile after Task 4.

- [ ] **Step 3: Commit**

```bash
git add src/server.go
git commit -m "feat: add HTTP server with create/status endpoints"
```

---

### Task 4: Add `NewOrchestratorFromSpecs` to generator package

The server pre-parses SQL for validation, so the Orchestrator needs a constructor that accepts already-parsed specs instead of a SQL file path.

**Files:**
- Modify: `src/generator/base_generator.go`

- [ ] **Step 1: Add NewOrchestratorFromSpecs**

Add the following function after the existing `NewOrchestrator` in `src/generator/base_generator.go`:

```go
// NewOrchestratorFromSpecs creates an orchestrator from pre-parsed column specs.
func NewOrchestratorFromSpecs(cfg *config.Config, specs []*spec.ColumnSpec) (*Orchestrator, error) {
	gen, err := newGenerator(cfg, specs)
	if err != nil {
		return nil, err
	}

	store, err := config.GetStore(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	logger := util.InitializeProgressLogger(
		cfg.Common.EndFileNo-cfg.Common.StartFileNo,
		"writing",
		time.Second,
	)
	logger.SetContext(
		strings.ToLower(cfg.Common.FileFormat),
		resolvePlatform(cfg),
	)

	return &Orchestrator{
		FileGenerator: gen,
		cfg:           cfg,
		store:         store,
		logger:        logger,
	}, nil
}
```

- [ ] **Step 2: Verify build**

Run: `cd /mnt/data/joechenrh/data-writer && make build`
Expected: Compiles successfully (all references from server.go now resolve).

- [ ] **Step 3: Commit**

```bash
git add src/generator/base_generator.go
git commit -m "feat: add NewOrchestratorFromSpecs constructor"
```

---

### Task 5: Add `-serve` and `-port` flags to main.go

Wire up the server mode entry point.

**Files:**
- Modify: `src/main.go`

- [ ] **Step 1: Add flags and server branch**

In `src/main.go`, add two new flags and a branch before the existing CLI logic. Replace the `main` function with:

```go
func main() {
	serve := flag.Bool("serve", false, "start HTTP server mode")
	port := flag.Int("port", 8080, "HTTP server port (only used with -serve)")
	operation := flag.String("op", "create", "create/delete/show/ls/upload/download, default is create")
	sqlPath := flag.String("sql", "", "sql path")
	cfgPath := flag.String("cfg", "", "config path")
	threads := flag.Int("threads", 16, "threads")
	localDir := flag.String("dir", "", "local directory for upload/download operation")
	cpuProfile := flag.String("cpuprofile", "", "write cpu profile to file (or use CPUPROFILE env var)")
	showSpec := flag.Bool("show-spec", false, "print parsed schema spec and exit")

	flag.Parse()

	if *serve {
		startServer(*port)
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
```

- [ ] **Step 2: Verify build**

Run: `cd /mnt/data/joechenrh/data-writer && make build`
Expected: Compiles successfully.

- [ ] **Step 3: Quick smoke test**

Run: `cd /mnt/data/joechenrh/data-writer && timeout 2 ./bin/data-writer -serve -port 9999 || true`
Expected: Prints "Starting server on :9999" then exits after timeout.

- [ ] **Step 4: Commit**

```bash
git add src/main.go
git commit -m "feat: add -serve and -port flags for HTTP server mode"
```

---

### Task 6: Create the frontend

Static HTML/CSS/JS dashboard with a form and status display.

**Files:**
- Create: `public/index.html`
- Create: `public/style.css`
- Create: `public/app.js`

- [ ] **Step 1: Create public directory**

```bash
mkdir -p /mnt/data/joechenrh/data-writer/public
```

- [ ] **Step 2: Create index.html**

Create `public/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Data Writer</title>
  <link rel="stylesheet" href="style.css">
</head>
<body>
  <div class="container">
    <h1>Data Writer</h1>

    <form id="createForm">
      <div class="form-group">
        <label for="sql">SQL Schema</label>
        <textarea id="sql" rows="8" placeholder="CREATE TABLE t1 (...)"></textarea>
      </div>

      <div class="form-row">
        <div class="form-group">
          <label for="path">Storage Path</label>
          <input type="text" id="path" placeholder="s3://bucket/path or /local/path">
        </div>
        <div class="form-group">
          <label for="prefix">File Prefix</label>
          <input type="text" id="prefix" value="test.t1">
        </div>
      </div>

      <div class="form-row">
        <div class="form-group">
          <label for="startFileNo">Start File No</label>
          <input type="number" id="startFileNo" value="0" min="0">
        </div>
        <div class="form-group">
          <label for="endFileNo">End File No</label>
          <input type="number" id="endFileNo" value="100" min="1">
        </div>
        <div class="form-group">
          <label for="rows">Rows per File</label>
          <input type="number" id="rows" value="60000" min="1">
        </div>
        <div class="form-group">
          <label for="format">Format</label>
          <select id="format">
            <option value="csv">CSV</option>
            <option value="parquet">Parquet</option>
          </select>
        </div>
      </div>

      <div id="s3Config" class="credential-section" style="display:none">
        <h3>S3 Credentials</h3>
        <div class="form-row">
          <div class="form-group">
            <label for="s3Region">Region</label>
            <input type="text" id="s3Region" placeholder="us-east-1">
          </div>
          <div class="form-group">
            <label for="s3Provider">Provider</label>
            <input type="text" id="s3Provider" placeholder="aws">
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label for="s3AccessKey">Access Key</label>
            <input type="text" id="s3AccessKey">
          </div>
          <div class="form-group">
            <label for="s3SecretKey">Secret Key</label>
            <input type="password" id="s3SecretKey">
          </div>
        </div>
        <div class="form-group">
          <label for="s3Endpoint">Endpoint</label>
          <input type="text" id="s3Endpoint" placeholder="https://s3.amazonaws.com">
        </div>
      </div>

      <div id="gcsConfig" class="credential-section" style="display:none">
        <h3>GCS Credentials</h3>
        <div class="form-group">
          <label for="gcsCredential">Credential File Path</label>
          <input type="text" id="gcsCredential" placeholder="/path/to/service-account.json">
        </div>
      </div>

      <button type="submit" id="submitBtn">Generate</button>
    </form>

    <details class="reference-section">
      <summary>Column Comment Options</summary>
      <div class="reference-content">
        <p>Customize data generation per column using SQL column comments:</p>
        <table class="reference-table">
          <thead>
            <tr><th>Option</th><th>Description</th><th>Example</th></tr>
          </thead>
          <tbody>
            <tr>
              <td><code>null_percent</code></td>
              <td>Percentage of NULL values</td>
              <td><code>COMMENT 'null_percent=20'</code></td>
            </tr>
            <tr>
              <td><code>max_length</code></td>
              <td>Max length for string types</td>
              <td><code>COMMENT 'max_length=120'</code></td>
            </tr>
            <tr>
              <td><code>min_length</code></td>
              <td>Min length for string types</td>
              <td><code>COMMENT 'min_length=60'</code></td>
            </tr>
            <tr>
              <td><code>mean</code></td>
              <td>Mean for numeric distributions</td>
              <td><code>COMMENT 'mean=100'</code></td>
            </tr>
            <tr>
              <td><code>stddev</code></td>
              <td>Standard deviation for numerics</td>
              <td><code>COMMENT 'stddev=15'</code></td>
            </tr>
            <tr>
              <td><code>compress</code></td>
              <td>Compression ratio hint (1-100)</td>
              <td><code>COMMENT 'compress=40'</code></td>
            </tr>
            <tr>
              <td><code>set</code></td>
              <td>Allowed values (JSON array)</td>
              <td><code>COMMENT 'set=["ok","warn","fail"]'</code></td>
            </tr>
            <tr>
              <td><code>order</code></td>
              <td>Integer ordering</td>
              <td><code>COMMENT 'order=total_order'</code></td>
            </tr>
          </tbody>
        </table>
        <p class="reference-example">Combined: <code>`c` char(120) COMMENT 'max_length=120, min_length=120'</code></p>
        <p class="reference-example"><code>`text_0` text COMMENT 'max_length=20000, compress=40'</code></p>
        <p class="reference-example"><code>`score` int COMMENT 'order=total_order, mean=100, stddev=15'</code></p>
      </div>
    </details>

    <div id="status" class="status-section" style="display:none">
      <h2>Task Status</h2>
      <div class="progress-bar-container">
        <div class="progress-bar" id="progressBar"></div>
        <span class="progress-text" id="progressText">0%</span>
      </div>
      <div class="status-details">
        <div class="status-item">
          <span class="label">State</span>
          <span id="stateText" class="value">-</span>
        </div>
        <div class="status-item">
          <span class="label">Files</span>
          <span id="filesText" class="value">-</span>
        </div>
        <div class="status-item">
          <span class="label">Written</span>
          <span id="sizeText" class="value">-</span>
        </div>
        <div class="status-item">
          <span class="label">Error</span>
          <span id="errorText" class="value error">-</span>
        </div>
      </div>
    </div>
  </div>
  <script src="app.js"></script>
</body>
</html>
```

- [ ] **Step 3: Create style.css**

Create `public/style.css`:

```css
* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #0f1117;
  color: #e1e4e8;
  padding: 2rem;
}

.container {
  max-width: 720px;
  margin: 0 auto;
}

h1 {
  font-size: 1.5rem;
  margin-bottom: 1.5rem;
  color: #f0f0f0;
}

h2 {
  font-size: 1.1rem;
  margin-bottom: 1rem;
  color: #f0f0f0;
}

h3 {
  font-size: 0.95rem;
  margin-bottom: 0.75rem;
  color: #b0b0b0;
}

.form-group {
  margin-bottom: 0.75rem;
  flex: 1;
}

.form-row {
  display: flex;
  gap: 0.75rem;
}

label {
  display: block;
  font-size: 0.8rem;
  color: #8b949e;
  margin-bottom: 0.25rem;
}

input, textarea, select {
  width: 100%;
  padding: 0.5rem;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 4px;
  color: #e1e4e8;
  font-size: 0.85rem;
  font-family: 'SFMono-Regular', Consolas, monospace;
}

textarea { resize: vertical; }

input:focus, textarea:focus, select:focus {
  outline: none;
  border-color: #58a6ff;
}

.credential-section {
  border: 1px solid #30363d;
  border-radius: 4px;
  padding: 0.75rem;
  margin-bottom: 0.75rem;
}

button {
  width: 100%;
  padding: 0.6rem;
  background: #238636;
  color: #fff;
  border: none;
  border-radius: 4px;
  font-size: 0.9rem;
  cursor: pointer;
  margin-top: 0.5rem;
}

button:hover { background: #2ea043; }
button:disabled { background: #21262d; color: #484f58; cursor: not-allowed; }

.reference-section {
  margin-top: 1rem;
  border: 1px solid #30363d;
  border-radius: 4px;
}

.reference-section > summary {
  padding: 0.6rem 0.75rem;
  cursor: pointer;
  font-size: 0.85rem;
  color: #8b949e;
  list-style: none;
}

.reference-section > summary::-webkit-details-marker { display: none; }
.reference-section > summary::marker { display: none; content: ''; }

.reference-section > summary::before {
  content: '\25B6';
  display: inline-block;
  margin-right: 0.5rem;
  font-size: 0.6rem;
  transition: transform 0.2s;
}

.reference-section[open] > summary::before {
  transform: rotate(90deg);
}

.reference-content {
  padding: 0 0.75rem 0.75rem;
  font-size: 0.8rem;
  color: #b0b0b0;
}

.reference-content p {
  margin-bottom: 0.5rem;
}

.reference-table {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 0.75rem;
}

.reference-table th,
.reference-table td {
  text-align: left;
  padding: 0.3rem 0.5rem;
  border-bottom: 1px solid #21262d;
  font-size: 0.8rem;
}

.reference-table th {
  color: #8b949e;
  font-weight: 600;
}

.reference-table code {
  background: #161b22;
  padding: 0.1rem 0.3rem;
  border-radius: 3px;
  font-size: 0.75rem;
}

.reference-example {
  font-size: 0.75rem;
  color: #8b949e;
}

.reference-example code {
  background: #161b22;
  padding: 0.1rem 0.3rem;
  border-radius: 3px;
}

.status-section {
  margin-top: 1.5rem;
  border: 1px solid #30363d;
  border-radius: 4px;
  padding: 1rem;
}

.progress-bar-container {
  position: relative;
  height: 24px;
  background: #161b22;
  border-radius: 4px;
  overflow: hidden;
  margin-bottom: 1rem;
}

.progress-bar {
  height: 100%;
  background: #238636;
  width: 0%;
  transition: width 0.3s;
}

.progress-text {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  font-size: 0.75rem;
  font-weight: 600;
}

.status-details {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.5rem;
}

.status-item {
  display: flex;
  justify-content: space-between;
  font-size: 0.8rem;
  padding: 0.3rem 0;
}

.status-item .label { color: #8b949e; }
.status-item .value { color: #e1e4e8; }
.status-item .error { color: #f85149; }
```

- [ ] **Step 4: Create app.js**

Create `public/app.js`:

```javascript
const form = document.getElementById('createForm');
const submitBtn = document.getElementById('submitBtn');
const statusSection = document.getElementById('status');
const pathInput = document.getElementById('path');
const s3Config = document.getElementById('s3Config');
const gcsConfig = document.getElementById('gcsConfig');

let pollTimer = null;

pathInput.addEventListener('input', function() {
  const v = this.value.toLowerCase();
  s3Config.style.display = v.startsWith('s3://') ? 'block' : 'none';
  gcsConfig.style.display = v.startsWith('gcs://') ? 'block' : 'none';
});

form.addEventListener('submit', async function(e) {
  e.preventDefault();

  const body = {
    sql: document.getElementById('sql').value,
    path: document.getElementById('path').value,
    prefix: document.getElementById('prefix').value,
    start_fileno: parseInt(document.getElementById('startFileNo').value, 10),
    end_fileno: parseInt(document.getElementById('endFileNo').value, 10),
    rows: parseInt(document.getElementById('rows').value, 10),
    format: document.getElementById('format').value,
  };

  if (pathInput.value.toLowerCase().startsWith('s3://')) {
    body.s3 = {
      region: document.getElementById('s3Region').value,
      access_key: document.getElementById('s3AccessKey').value,
      secret_key: document.getElementById('s3SecretKey').value,
      provider: document.getElementById('s3Provider').value,
      endpoint: document.getElementById('s3Endpoint').value,
    };
  } else if (pathInput.value.toLowerCase().startsWith('gcs://')) {
    body.gcs = {
      credential: document.getElementById('gcsCredential').value,
    };
  }

  try {
    const resp = await fetch('/api/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok) {
      alert('Error: ' + (data.error || 'Unknown error'));
      return;
    }

    submitBtn.disabled = true;
    statusSection.style.display = 'block';
    startPolling();
  } catch (err) {
    alert('Request failed: ' + err.message);
  }
});

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(pollStatus, 2000);
  pollStatus();
}

async function pollStatus() {
  try {
    const resp = await fetch('/api/status');
    const data = await resp.json();

    document.getElementById('stateText').textContent = data.state;
    document.getElementById('progressText').textContent = data.progress;
    document.getElementById('progressBar').style.width = data.progress;
    document.getElementById('filesText').textContent =
      data.files_written + ' / ' + data.total_files;
    document.getElementById('sizeText').textContent = data.written_size || '-';
    document.getElementById('errorText').textContent = data.error || '-';

    if (data.state === 'completed' || data.state === 'failed') {
      clearInterval(pollTimer);
      pollTimer = null;
      submitBtn.disabled = false;
    }
  } catch (err) {
    console.error('Poll failed:', err);
  }
}
```

- [ ] **Step 5: Verify build**

Run: `cd /mnt/data/joechenrh/data-writer && make build`
Expected: Compiles successfully (embed picks up public/ directory).

- [ ] **Step 6: Commit**

```bash
git add public/ src/server.go
git commit -m "feat: add dashboard frontend"
```

Note: `src/server.go` is included here if it wasn't committed in Task 3 (due to compile dependency on Task 4). If it was already committed, just `git add public/`.

---

### Task 7: Add S3 JSON tags to config structs

The `S3Config` and `GCSConfig` structs use `toml` tags but the server sends JSON. Add `json` tags so `encoding/json` can decode them.

**Files:**
- Modify: `src/config/config.go`

- [ ] **Step 1: Add json tags to S3Config and GCSConfig**

In `src/config/config.go`, update the struct tags:

```go
type S3Config struct {
	Region          string `toml:"region,omitempty" json:"region"`
	AccessKey       string `toml:"access_key,omitempty" json:"access_key"`
	SecretAccessKey string `toml:"secret_key,omitempty" json:"secret_key"`
	Provider        string `toml:"provider,omitempty" json:"provider"`
	Endpoint        string `toml:"endpoint,omitempty" json:"endpoint"`
	Force           bool   `toml:"force,omitempty" json:"force"`
	RoleArn         string `toml:"role_arn,omitempty" json:"role_arn"`
}

type GCSConfig struct {
	Credential string `toml:"credential,omitempty" json:"credential"`
}
```

- [ ] **Step 2: Verify build**

Run: `cd /mnt/data/joechenrh/data-writer && make build`
Expected: Compiles successfully.

- [ ] **Step 3: Commit**

```bash
git add src/config/config.go
git commit -m "feat: add json tags to S3/GCS config structs"
```

---

### Task 8: End-to-end test

Verify the server works with a local-path generation task.

**Files:** None (manual test)

- [ ] **Step 1: Build**

```bash
cd /mnt/data/joechenrh/data-writer && make build
```

- [ ] **Step 2: Start server**

In a terminal:
```bash
./bin/data-writer -serve -port 8080
```

Expected: `Starting server on :8080`

- [ ] **Step 3: Test static page**

```bash
curl -s http://localhost:8080/ | head -5
```

Expected: Returns `<!DOCTYPE html>` and the page HTML.

- [ ] **Step 4: Test create with local path**

```bash
curl -s -X POST http://localhost:8080/api/create \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "CREATE TABLE t1 (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, k int NOT NULL DEFAULT 0)",
    "path": "/tmp/data-writer-test",
    "prefix": "test.t1",
    "start_fileno": 0,
    "end_fileno": 2,
    "rows": 100,
    "format": "csv"
  }'
```

Expected: `{"status":"started"}`

- [ ] **Step 5: Poll status**

```bash
sleep 3 && curl -s http://localhost:8080/api/status | python3 -m json.tool
```

Expected: State should be `completed`, files_written should be `2`.

- [ ] **Step 6: Verify conflict rejection**

Start a long task, then try submitting another:

```bash
curl -s -X POST http://localhost:8080/api/create \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "CREATE TABLE t1 (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, k int NOT NULL DEFAULT 0)",
    "path": "/tmp/data-writer-test2",
    "prefix": "test.t1",
    "start_fileno": 0,
    "end_fileno": 1000,
    "rows": 60000,
    "format": "csv"
  }'

# Immediately try another:
curl -s -X POST http://localhost:8080/api/create \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE TABLE t(id int)","path":"/tmp/x","prefix":"x","start_fileno":0,"end_fileno":1,"rows":1,"format":"csv"}'
```

Expected: Second call returns `409` with `{"error":"task already running"}`.

- [ ] **Step 7: Clean up test data**

```bash
rm -rf /tmp/data-writer-test /tmp/data-writer-test2
```

---

### Task 9: Update Cloudflare Tunnel config

Add the new service to the Cloudflare Tunnel so it's accessible via domain.

**Files:**
- Modify: `~/.cloudflared/config.yml`

- [ ] **Step 1: Add ingress rule**

Edit `~/.cloudflared/config.yml` to add a new ingress rule before the catch-all:

```yaml
  - hostname: datagen.ingresses.org
    service: http://localhost:8080
```

The full file should look like:

```yaml
tunnel: 8662cb9d-d007-4cb8-8f8c-f339406d4b3b
credentials-file: /mnt/data/joechenrh/.cloudflared/8662cb9d-d007-4cb8-8f8c-f339406d4b3b.json

ingress:
  - hostname: raas.ingresses.org
    service: http://localhost:6688
  - hostname: import.ingresses.org
    service: http://localhost:8080
  - hostname: datagen.ingresses.org
    service: http://localhost:8080
  - service: http_status:404
```

**Note:** `import.ingresses.org` and `datagen.ingresses.org` both point to port 8080. If the import service is still needed on 8080, choose a different port for data-writer (e.g., `-port 8081`) and update accordingly. Confirm with user before applying.

- [ ] **Step 2: Add DNS record in Cloudflare**

This must be done manually in the Cloudflare dashboard — add a CNAME record for `datagen` pointing to the tunnel.

- [ ] **Step 3: Restart cloudflared**

Restart the cloudflared tunnel process to pick up the config change.
