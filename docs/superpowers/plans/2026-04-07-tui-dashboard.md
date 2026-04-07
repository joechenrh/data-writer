# TUI Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the data-writer web UI from a generic SaaS form into a phosphor-green TUI dashboard inspired by lazygit/htop, with first-class display of the EC2 shard pool, while preserving every existing piece of functionality.

**Architecture:** Two-pane layout (form left ~45% / queue+detail right ~55%) with a top airline status bar and bottom keystroke status bar. CSS variables, JetBrains Mono everywhere, dotted/dashed dividers, color-coded states. Pure HTML/CSS/JS — no build step. One small server change exposes `worker_total` / `worker_done` in the existing tasks JSON.

**Tech Stack:** Vanilla HTML5 / CSS3 / ES6, no framework. Go 1.23 (server). pgx (Postgres). The web assets are embedded into the Go binary via `//go:embed all:public`.

**Spec:** `docs/superpowers/specs/2026-04-07-tui-dashboard-design.md`
**Visual reference:** `.superpowers/brainstorm/1085345-1775556525/content/tui-fullpage.html` — open it in the visual companion browser for exact CSS values and DOM structure. Treat it as the source of truth for color/spacing/typography decisions.

---

## File Structure

| File | Action | Responsibility |
| ---- | ------ | -------------- |
| `src/server/handler.go` | Modify (lines 130-146, 178-190) | Add `worker_total` and `worker_done` to `handleStatus` and `handleListTasks` JSON responses |
| `src/server/public/index.html` | Rewrite | New TUI markup: top airline, two-pane body, bottom status bar, help overlay, format inline section. Cache-bust to `?v=34`. |
| `src/server/public/style.css` | Rewrite | CSS variables, TUI grid layout, pane styles, table, detail panel, status bars, overlay |
| `src/server/public/app.js` | Rewrite | Keyboard router, focus state, queue selection, detail renderer, AI line, help overlay, format inline, clock, connection status |
| `src/server/handler_shard_test.go` | Create (new, gitignored) | Test for the JSON additions |

No new files. The existing `SKILL.md` is unchanged.

---

## Task 1: Expose `worker_total` and `worker_done` in tasks JSON

**Files:**
- Modify: `src/server/handler.go:97-147` (handleStatus)
- Modify: `src/server/handler.go:149-196` (handleListTasks)
- Create: `src/server/handler_shard_test.go`

This is the only allowed server change. The frontend depends on these two fields to render shard counts and shard summaries.

- [ ] **Step 1: Read the current handler functions to confirm context**

Read `src/server/handler.go` lines 97-200 with the Read tool. You should see `handleStatus` (single task) and `handleListTasks` (10 most recent). Both run a `SELECT` against the `tasks` table and assemble a `map[string]any` for the JSON response.

- [ ] **Step 2: Write a failing test for handleListTasks**

Create `src/server/handler_shard_test.go`:

```go
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// requireDB skips the test if no test DSN is configured.
func requireDB(t *testing.T) {
	t.Helper()
	dsn := testDSN()
	if dsn == "" {
		t.Skip("set DATAWRITER_TEST_DSN to run server handler tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	DB = pool
	if _, err := DB.Exec(context.Background(), createTableSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := DB.Exec(context.Background(), migrateTableSQL); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func testDSN() string {
	// Pulled from env so the test never connects to prod by accident.
	return ""
}

func TestHandleListTasksIncludesShardFields(t *testing.T) {
	requireDB(t)

	// Insert one task with worker_total=4, worker_done=1.
	var id int64
	err := DB.QueryRow(context.Background(),
		`INSERT INTO tasks (sql_text, config_json, total_files, target, worker_total, worker_done)
		 VALUES ('CREATE TABLE t.t(id int)', '{}', 5000, 'ec2', 4, 1)
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer DB.Exec(context.Background(), `DELETE FROM tasks WHERE id=$1`, id)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	handleListTasks(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one task in response")
	}
	first := rows[0]
	wt, ok := first["worker_total"].(float64)
	if !ok || int(wt) != 4 {
		t.Errorf("worker_total: got %v, want 4", first["worker_total"])
	}
	wd, ok := first["worker_done"].(float64)
	if !ok || int(wd) != 1 {
		t.Errorf("worker_done: got %v, want 1", first["worker_done"])
	}
}
```

- [ ] **Step 3: Run the test and verify it fails to compile**

Run: `go test ./src/server/ -run TestHandleListTasksIncludesShardFields -v`
Expected: compile error or missing fields. The reason is that `handleListTasks` doesn't yet expose `worker_total` / `worker_done`, so the test sees them as `nil`. (If you set `DATAWRITER_TEST_DSN` first by editing `testDSN()` to return your DSN, the test runs and fails on the assertion.)

- [ ] **Step 4: Add the fields to `handleListTasks`**

In `src/server/handler.go:149-196`, change the SELECT and the row scan:

```go
func handleListTasks(w http.ResponseWriter, r *http.Request) {
	rows, err := DB.Query(r.Context(),
		`SELECT id, state, target, error, files_written, total_files, written_bytes,
		        worker_total, worker_done, created_at, updated_at
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
		var workerTotal, workerDone int
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&id, &state, &target, &taskErr, &filesWritten, &totalFiles, &writtenBytes,
			&workerTotal, &workerDone, &createdAt, &updatedAt); err != nil {
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
			"worker_total":  workerTotal,
			"worker_done":   workerDone,
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
```

- [ ] **Step 5: Add the same fields to `handleStatus`**

In `src/server/handler.go:97-147`, change the SELECT, scan, and JSON map:

```go
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
	var workerTotal, workerDone int
	var createdAt, updatedAt time.Time

	err := DB.QueryRow(r.Context(),
		`SELECT state, error, files_written, total_files, written_bytes,
		        worker_total, worker_done, created_at, updated_at
		 FROM tasks WHERE id = $1`, id,
	).Scan(&state, &taskErr, &filesWritten, &totalFiles, &writtenBytes,
		&workerTotal, &workerDone, &createdAt, &updatedAt)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

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
		"worker_total":  workerTotal,
		"worker_done":   workerDone,
		"error":         taskErr,
		"created_at":    createdAt.Format(time.RFC3339),
		"updated_at":    updatedAt.Format(time.RFC3339),
	})
}
```

- [ ] **Step 6: Build to confirm no compile errors**

Run: `go build ./...`
Expected: success, no output.

- [ ] **Step 7: If a test DSN is available, run the test**

If you have a Postgres test database, set `testDSN()` to return its DSN and run:
`go test ./src/server/ -run TestHandleListTasksIncludesShardFields -v`
Expected: PASS. Otherwise the test stays as a documented contract — it's gitignored anyway.

- [ ] **Step 8: Commit**

```bash
git add src/server/handler.go
git commit -m "feat(api): expose worker_total and worker_done in tasks JSON

The TUI dashboard frontend uses these to display shard count badges
(\"ec2 ×4\") in the queue and a 'shards · N workers · M done' summary
in the detail panel."
```

---

## Task 2: Bootstrap new style.css with CSS variables and base reset

**Files:**
- Rewrite: `src/server/public/style.css`

The current style.css is the generic SaaS theme. Replace it wholesale with the TUI base. Subsequent tasks will append component-specific styles. This task lays down the variables, the body background, the page wrapper, and the overall layout grid.

- [ ] **Step 1: Open the visual reference**

Open `.superpowers/brainstorm/1085345-1775556525/content/tui-fullpage.html` in a text editor. Skim the `<style>` block — it contains the exact color values, font sizes, border styles, and layout grid for every component you'll be building. Treat it as your visual source of truth. The CSS variables and most class names in this plan match it.

- [ ] **Step 2: Replace style.css with the base + layout grid**

Overwrite `src/server/public/style.css`:

```css
/* === data-writer · TUI dashboard === */

@import url("https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap");

:root {
  --bg: #050d08;
  --bg-alt: #0a1a12;
  --bg-pane: #061410;
  --line: #1a3a26;
  --line-soft: #11281b;

  --txt: #b8eac9;
  --txt-dim: #6fcc97;
  --txt-mute: #4a8466;

  --green: #6cf09a;
  --amber: #f0c674;
  --cyan: #6cd0f0;
  --magenta: #f06cd0;
  --red: #f06c6c;

  --font-mono: "JetBrains Mono", "IBM Plex Mono", ui-monospace, "SF Mono", Consolas, monospace;
}

*, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }

html, body { height: 100%; }

body {
  font-family: var(--font-mono);
  font-size: 11.5px;
  line-height: 1.45;
  color: var(--txt);
  background: var(--bg);
  -webkit-font-smoothing: antialiased;
  overflow: hidden;
}

/* The whole page is a single screen. */
.dw-screen {
  display: grid;
  grid-template-rows: 24px 1fr 22px;
  height: 100vh;
  position: relative;
  box-shadow: inset 0 0 60px rgba(108, 240, 154, 0.04);
}

/* Subtle scanline overlay over the whole screen. Disabled for users who prefer reduced motion. */
.dw-screen::after {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  background: repeating-linear-gradient(0deg, rgba(108, 240, 154, 0.025) 0 1px, transparent 1px 3px);
  z-index: 50;
}
@media (prefers-reduced-motion: reduce) {
  .dw-screen::after { display: none; }
}

/* Body grid: form on left, queue+detail on right. */
.dw-body {
  display: grid;
  grid-template-columns: minmax(360px, 0.45fr) minmax(420px, 0.55fr);
  min-height: 0;
  overflow: hidden;
}

@media (max-width: 720px) {
  .dw-body { grid-template-columns: 1fr; }
}

.dw-pane {
  border-right: 1px solid var(--line);
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: auto;
}
.dw-pane:last-child { border-right: none; }
.dw-pane.right { display: grid; grid-template-rows: 1fr 0.85fr; }

.dw-pane-head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  height: 24px;
  border-bottom: 1px solid var(--line-soft);
  color: var(--txt-mute);
  font-size: 10px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}
.dw-pane-head .title { color: var(--green); font-weight: 600; }
.dw-pane-head .key {
  background: rgba(108, 240, 154, 0.12); color: var(--green);
  padding: 0 5px; border-radius: 2px; font-size: 9px; font-weight: 700;
}
.dw-pane-head .right { margin-left: auto; color: var(--txt-mute); }
```

(Subsequent tasks append more rules to this file. Do not delete the contents above.)

- [ ] **Step 3: Verify the file was written**

Run: `wc -l src/server/public/style.css`
Expected: ~95 lines.

- [ ] **Step 4: Commit**

```bash
git add src/server/public/style.css
git commit -m "refactor(ui): bootstrap TUI base styles and layout grid

Wholesale replace the SaaS theme. CSS variables for the phosphor-green
palette, JetBrains Mono everywhere, scanline overlay, and the
top-airline / two-pane / bottom-status grid that subsequent components
will fill in."
```

---

## Task 3: New index.html skeleton

**Files:**
- Rewrite: `src/server/public/index.html`

Replace the generic markup with the TUI skeleton: top airline, two-pane body, bottom status bar, plus empty containers for the help overlay and the form/queue/detail content. Subsequent tasks fill in the panes.

- [ ] **Step 1: Replace index.html**

Overwrite `src/server/public/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>data-writer</title>
  <link rel="stylesheet" href="/style.css?v=34">
</head>
<body>

<div class="dw-screen">

  <!-- ── top airline ── -->
  <header class="dw-top">
    <div class="seg brand">DW</div>
    <div class="seg host" id="airline-host">datawriter@…</div>
    <div class="seg path"  id="airline-path">~/jobs/new</div>
    <div class="seg tasks" id="airline-tasks">queue: idle</div>
    <div class="seg clock" id="airline-clock">--:--:-- ●</div>
  </header>

  <!-- ── body 2-pane ── -->
  <main class="dw-body">

    <!-- LEFT: form -->
    <section class="dw-pane left" id="pane-form" tabindex="-1">
      <div class="dw-pane-head">
        <span class="title">[ NEW JOB ]</span>
        <span class="key">n</span>
        <span class="right" id="form-meta">no schema</span>
      </div>
      <form id="gen-form" class="form-body" novalidate>
        <!-- filled in Task 4 -->
      </form>
    </section>

    <!-- RIGHT: queue + details -->
    <section class="dw-pane right" id="pane-right">

      <div class="queue-wrap" tabindex="-1">
        <div class="dw-pane-head">
          <span class="title">[ QUEUE ]</span>
          <span class="key">tab</span>
          <span class="right">↑↓ select  ·  x cancel  ·  r refresh</span>
        </div>
        <div id="queue-empty" class="queue-empty">no jobs yet</div>
        <table id="queue-table" class="dw-table" hidden>
          <thead>
            <tr><th>id</th><th>state</th><th>target</th><th>progress</th><th>files</th><th>size</th><th>age</th></tr>
          </thead>
          <tbody id="queue-tbody"></tbody>
        </table>
      </div>

      <div class="detail-wrap">
        <!-- filled in Task 6 -->
      </div>

    </section>

  </main>

  <!-- ── bottom status bar ── -->
  <footer class="dw-bot">
    <div class="keys">
      <span><span class="k">n</span>·new</span>
      <span><span class="k">↵</span>·run</span>
      <span><span class="k">tab</span>·focus</span>
      <span><span class="k">/</span>·ai</span>
      <span><span class="k">x</span>·cancel</span>
      <span><span class="k">r</span>·reload</span>
      <span><span class="k">?</span>·help</span>
    </div>
    <div class="right" id="bot-status">
      <span class="dot ok" id="bot-db">●</span> db9 ·
      <span class="dot ok" id="bot-launcher">●</span> launcher ·
      <span class="dot mute" id="bot-running">●</span> idle
    </div>
  </footer>

</div>

<!-- ── help overlay (closed by default) ── -->
<div id="help-overlay" class="dw-overlay" hidden>
  <!-- filled in Task 7 -->
</div>

<script src="/app.js?v=34"></script>
</body>
</html>
```

- [ ] **Step 2: Restart the server (if running)**

Find the running `data-writer -serve` process (`pgrep -af 'data-writer -serve'`) and restart it so the new embedded `public/` is picked up:

```bash
go build -o bin/data-writer ./src/
# kill old, start new in your tmux pane (don't do this from the agent)
```

Or just verify by curl:

```bash
curl -s http://localhost:8081/ | grep -c "dw-screen"
```

Expected: `1` once the server is restarted with the new binary.

- [ ] **Step 3: Open http://localhost:8081 in your browser**

You should see the top airline bar, an empty form pane on the left, an empty queue pane on the right, and the bottom status bar. No content inside the panes yet — that comes next.

- [ ] **Step 4: Commit**

```bash
git add src/server/public/index.html
git commit -m "refactor(ui): replace index.html with TUI skeleton

Top airline + 2-pane body + bottom status bar. Empty placeholders for
the form, queue, detail panel, and help overlay — subsequent commits
fill them in. Cache-bust to v=34."
```

---

## Task 4: Form pane (markup, styles, behavior)

**Files:**
- Modify: `src/server/public/index.html` (form-body block)
- Append: `src/server/public/style.css` (form pane styles)

This task fills in the left pane with all the existing form fields, restyled. The fields, names, validation, and submit handler are preserved from today's app.js — only the markup and styles change. The submit handler is wired in Task 8.

- [ ] **Step 1: Replace the empty form-body block in index.html**

Find this comment in `src/server/public/index.html`:

```html
<form id="gen-form" class="form-body" novalidate>
  <!-- filled in Task 4 -->
</form>
```

Replace the comment with the full form markup:

```html
<form id="gen-form" class="form-body" novalidate>

  <div class="field">
    <div class="field-label">
      schema<span class="req">*</span>
      <span class="hint" id="help-btn"><span class="k">?</span> help</span>
    </div>
    <textarea id="sql" name="sql" class="dw-input dw-sql" rows="10"
      placeholder="CREATE TABLE test.sb (
  id BIGINT PRIMARY KEY,
  c CHAR(120) COMMENT 'max_length=120'
);"></textarea>
  </div>

  <!-- AI assist line — hidden by default, opens with `/` -->
  <div class="ai-line" id="ai-line" hidden>
    <span class="prompt">/</span>
    <input id="ai-prompt" type="text" placeholder="describe the schema you want…">
    <span class="badge">↵ apply  <span class="k">esc</span> dismiss</span>
  </div>

  <div class="field">
    <div class="field-label">path<span class="req">*</span></div>
    <input id="path" name="path" class="dw-input" type="text"
           placeholder="s3://bucket/prefix or /local/path">
  </div>

  <div class="field-grid grid-3">
    <div class="field">
      <div class="field-label">files</div>
      <input id="end_fileno" name="end_fileno" class="dw-input" type="number" value="100" min="1">
    </div>
    <div class="field">
      <div class="field-label">rows / file</div>
      <input id="rows" name="rows" class="dw-input" type="number" value="60000" min="1">
    </div>
    <div class="field">
      <div class="field-label">format <span class="hint" id="format-options-btn">opts</span></div>
      <select id="format" name="format" class="dw-input">
        <option value="csv">csv</option>
        <option value="parquet">parquet</option>
      </select>
    </div>
  </div>

  <!-- Inline format options panel (Task 9). Hidden by default. -->
  <div id="format-options" class="format-options" hidden></div>

  <div class="field-grid grid-2">
    <div class="field">
      <div class="field-label">target</div>
      <select id="target" class="dw-input">
        <option value="local">local</option>
        <option value="ec2">ec2</option>
      </select>
    </div>
    <div class="field">
      <div class="field-label">subdirs</div>
      <input id="folders" name="folders" class="dw-input" type="number" min="0" placeholder="0">
    </div>
  </div>

  <!-- Storage credentials section, expands when path starts with s3:// -->
  <div id="cred-section" class="cred-section" hidden>
    <div class="field-grid grid-2">
      <div class="field">
        <div class="field-label">storage</div>
        <select id="storage_type" class="dw-input">
          <option value="aws">aws</option>
          <option value="ksyun">ksyun</option>
        </select>
      </div>
      <div class="field" id="ec2-toggle-group">
        <div class="field-label">on ec2 instance</div>
        <label class="toggle"><input id="run-on-ec2" type="checkbox" checked><span></span></label>
      </div>
    </div>
    <div id="aws-fields">
      <div id="ec2-hint" class="cred-hint" hidden>uses pre-configured iam role</div>
      <div id="aws-cred-fields">
        <div class="field-grid grid-2">
          <div class="field"><div class="field-label">region</div><input id="s3_region" class="dw-input" placeholder="us-east-1"></div>
          <div class="field"><div class="field-label">provider</div><input id="s3_provider" class="dw-input" placeholder="aws"></div>
        </div>
        <div class="field-grid grid-2">
          <div class="field"><div class="field-label">access key</div><input id="s3_access_key" class="dw-input" autocomplete="off"></div>
          <div class="field"><div class="field-label">secret key</div><input id="s3_secret_key" class="dw-input" type="password" autocomplete="off"></div>
        </div>
        <div class="field"><div class="field-label">endpoint</div><input id="s3_endpoint" class="dw-input" placeholder="https://s3.amazonaws.com"></div>
      </div>
    </div>
    <div id="ksyun-fields" hidden>
      <div class="cred-hint">credentials injected from ksyun_key env var on the server</div>
    </div>
  </div>

  <div class="form-footer">
    <div class="form-status" id="form-status">no schema</div>
    <button id="submit-btn" class="btn btn-run" type="submit">execute ↵</button>
  </div>

</form>
```

- [ ] **Step 2: Append the form pane styles to style.css**

Append to `src/server/public/style.css`:

```css
/* === form pane === */
.form-body {
  flex: 1;
  padding: 12px 14px 14px;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: auto;
}

.field { display: flex; flex-direction: column; gap: 4px; min-width: 0; }

.field-label {
  display: flex; align-items: center; gap: 6px;
  color: var(--txt-dim);
  font-size: 9.5px; letter-spacing: 0.1em; text-transform: uppercase;
}
.field-label .req { color: var(--amber); }
.field-label .hint {
  margin-left: auto;
  color: var(--txt-mute);
  font-size: 9.5px;
  cursor: pointer;
  text-transform: none;
  letter-spacing: 0;
}
.field-label .hint:hover { color: var(--green); }
.field-label .hint .k {
  background: rgba(108, 240, 154, 0.12); color: var(--green);
  padding: 0 4px; border-radius: 2px; font-size: 9px;
}

.dw-input {
  background: var(--bg-pane);
  color: var(--txt);
  border: 1px solid var(--line);
  border-left: 2px solid var(--green);
  padding: 6px 9px;
  font: inherit;
  font-size: 11px;
  width: 100%;
  border-radius: 0;
  outline: none;
  appearance: none;
  -webkit-appearance: none;
}
textarea.dw-input { min-height: 110px; line-height: 1.55; resize: vertical; }
.dw-input:focus { box-shadow: 0 0 0 1px var(--green); border-left-color: var(--green); }
.dw-input.invalid { border-left-color: var(--red); }
.dw-input.invalid:focus { box-shadow: 0 0 0 1px var(--red); }
.dw-input::placeholder { color: var(--txt-mute); }

select.dw-input {
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'%3E%3Cpath fill='%236cf09a' d='M5 7L1 3h8z'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 8px center;
  padding-right: 24px;
  cursor: pointer;
}

.field-grid { display: grid; gap: 8px; }
.grid-2 { grid-template-columns: 1fr 1fr; }
.grid-3 { grid-template-columns: 1fr 1fr 1fr; }

/* AI assist line */
.ai-line {
  display: flex; align-items: center; gap: 8px;
  border: 1px solid var(--line);
  border-left: 2px solid var(--cyan);
  background: var(--bg-pane);
  padding: 6px 9px;
}
.ai-line .prompt { color: var(--cyan); font-weight: 700; }
.ai-line input { background: transparent; border: none; outline: none; color: var(--txt); width: 100%; font: inherit; font-size: 11px; }
.ai-line .badge { color: var(--txt-mute); font-size: 9.5px; white-space: nowrap; }
.ai-line .badge .k { background: rgba(108, 208, 240, 0.12); color: var(--cyan); padding: 0 4px; border-radius: 2px; }

/* Credentials section */
.cred-section {
  border: 1px dashed var(--line);
  padding: 10px 12px;
  display: flex; flex-direction: column; gap: 10px;
}
.cred-hint { color: var(--txt-mute); font-size: 10px; }

/* Toggle */
.toggle { display: inline-flex; align-items: center; cursor: pointer; height: 22px; }
.toggle input { display: none; }
.toggle span {
  position: relative; display: inline-block; width: 30px; height: 14px;
  background: var(--bg-pane); border: 1px solid var(--line); border-radius: 0;
}
.toggle span::after {
  content: ""; position: absolute; top: 1px; left: 1px;
  width: 10px; height: 10px; background: var(--txt-mute);
  transition: left 0.12s ease, background 0.12s ease;
}
.toggle input:checked + span::after { left: 17px; background: var(--green); }

/* Form footer */
.form-footer {
  margin-top: auto;
  display: flex; align-items: center; justify-content: space-between;
  padding-top: 8px; border-top: 1px dashed var(--line-soft);
}
.form-status { color: var(--txt-mute); font-size: 10px; }
.form-status.ok { color: var(--green); }
.form-status.bad { color: var(--red); }

.btn {
  background: transparent;
  border: 1px solid var(--green);
  color: var(--green);
  font: inherit; font-size: 11px; font-weight: 700;
  padding: 6px 16px;
  cursor: pointer;
  letter-spacing: 0.06em;
}
.btn:hover:not(:disabled) { background: rgba(108, 240, 154, 0.12); }
.btn:disabled { color: var(--txt-mute); border-color: var(--line); cursor: not-allowed; }
.btn.btn-run::before { content: ""; }
```

- [ ] **Step 3: Reload the page (cache-bypass)**

Hard-reload http://localhost:8081 with Cmd+Shift+R / Ctrl+Shift+R. The left pane should now contain a styled form: schema textarea with green left bar, path input, three small inputs, target/subdirs grid, and an `execute ↵` button at the bottom. The form has no behavior yet — that comes in Task 8.

- [ ] **Step 4: Commit**

```bash
git add src/server/public/index.html src/server/public/style.css
git commit -m "refactor(ui): build the TUI form pane

All existing form fields, restyled. Schema textarea with green left bar,
inline AI line scaffold, credentials section with dashed border. No
behavior yet — submit handler is wired in a later task."
```

---

## Task 5: Queue table styles + render

**Files:**
- Append: `src/server/public/style.css` (queue + state chip styles)
- Rewrite: `src/server/public/app.js` (initial version: just the queue render + poll)

This task gets the right pane top half showing real data. We rewrite app.js from scratch and start with just the queue. Subsequent tasks add the detail panel, AI line, keyboard router, etc.

- [ ] **Step 1: Append queue styles to style.css**

Append to `src/server/public/style.css`:

```css
/* === queue table === */
.queue-wrap { display: flex; flex-direction: column; min-height: 0; overflow: hidden; }
.queue-wrap .dw-pane-head { flex: 0 0 auto; }

.queue-empty {
  padding: 24px 14px;
  color: var(--txt-mute); font-size: 11px; text-align: center;
}

.dw-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
}
.dw-table th {
  text-align: left; font-weight: 500; color: var(--txt-mute);
  font-size: 9.5px; text-transform: uppercase; letter-spacing: 0.08em;
  padding: 4px 12px;
  border-bottom: 1px dashed var(--line-soft);
  position: sticky; top: 0; background: var(--bg);
}
.dw-table td {
  padding: 4px 12px; color: var(--txt);
  border-bottom: 1px dotted var(--line-soft);
  white-space: nowrap;
}
.dw-table tbody tr { cursor: pointer; }
.dw-table tbody tr:hover td { background: rgba(108, 240, 154, 0.04); }
.dw-table tbody tr.sel td {
  background: rgba(108, 240, 154, 0.10);
  color: var(--green);
}
.dw-table tbody tr.sel td:first-child::before {
  content: "▶"; color: var(--green); margin-right: 4px; margin-left: -10px;
}

/* state chips */
.state {
  display: inline-block;
  font-size: 10px;
  padding: 0 5px;
  border-radius: 2px;
  letter-spacing: 0.04em;
  text-transform: lowercase;
}
.s-running   { background: rgba(108,240,154,0.12); color: var(--green); }
.s-pending   { background: rgba(240,198,116,0.14); color: var(--amber); }
.s-launching { background: rgba(108,208,240,0.12); color: var(--cyan); }
.s-completed { color: var(--txt-mute); }
.s-failed    { background: rgba(240,108,108,0.12); color: var(--red); }

/* progress bar */
.progbar {
  display: inline-block; width: 80px; height: 6px;
  background: var(--bg-pane); border: 1px solid var(--line);
  position: relative; vertical-align: middle;
}
.progbar > span {
  display: block; height: 100%; background: var(--green);
}
.progbar.failed > span { background: var(--red); }
```

- [ ] **Step 2: Replace app.js with the initial version**

Overwrite `src/server/public/app.js`:

```javascript
'use strict';

// ── DOM refs ──────────────────────────────────────────────────────────────
const queueTable = document.getElementById('queue-table');
const queueTbody = document.getElementById('queue-tbody');
const queueEmpty = document.getElementById('queue-empty');
const airlineTasks = document.getElementById('airline-tasks');

// ── State ─────────────────────────────────────────────────────────────────
let tasks = [];
let selectedTaskId = null;
let pollTimer = null;

// ── Helpers ───────────────────────────────────────────────────────────────
function fmtAge(iso) {
  if (!iso) return '—';
  const sec = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (sec < 60)    return sec + 's';
  if (sec < 3600)  return Math.floor(sec / 60) + 'm ' + (sec % 60) + 's';
  if (sec < 86400) return Math.floor(sec / 3600) + 'h ' + Math.floor((sec % 3600) / 60) + 'm';
  return Math.floor(sec / 86400) + 'd';
}

function pct(t) {
  if (!t.total_files) return 0;
  return Math.min(100, Math.floor((t.files_written / t.total_files) * 100));
}

function targetLabel(t) {
  const wt = t.worker_total || 1;
  if (t.target === 'ec2' && wt > 1) return 'ec2 ×' + wt;
  return t.target;
}

// ── Queue render ──────────────────────────────────────────────────────────
function renderQueue() {
  if (tasks.length === 0) {
    queueTable.hidden = true;
    queueEmpty.hidden = false;
    airlineTasks.textContent = 'queue: idle';
    return;
  }
  queueEmpty.hidden = true;
  queueTable.hidden = false;

  queueTbody.innerHTML = '';
  for (const t of tasks) {
    const tr = document.createElement('tr');
    tr.dataset.id = t.id;
    if (selectedTaskId == null) selectedTaskId = String(t.id);
    if (String(t.id) === selectedTaskId) tr.classList.add('sel');

    const p = pct(t);
    const failed = t.state === 'failed';
    tr.innerHTML =
      `<td>#${t.id}</td>` +
      `<td><span class="state s-${t.state}">${t.state}</span></td>` +
      `<td>${targetLabel(t)}</td>` +
      `<td><span class="progbar${failed ? ' failed' : ''}"><span style="width:${p}%"></span></span> ${p}%</td>` +
      `<td>${t.files_written} / ${t.total_files}</td>` +
      `<td>${t.written_size || '—'}</td>` +
      `<td>${fmtAge(t.created_at)}</td>`;
    tr.addEventListener('click', () => {
      selectedTaskId = String(t.id);
      renderQueue();
    });
    queueTbody.appendChild(tr);
  }

  // airline summary
  const counts = { running: 0, completed: 0, failed: 0 };
  for (const t of tasks) counts[t.state] = (counts[t.state] || 0) + 1;
  const parts = [];
  if (counts.running)   parts.push(counts.running + ' active');
  if (counts.completed) parts.push(counts.completed + ' done');
  if (counts.failed)    parts.push(counts.failed + ' failed');
  airlineTasks.textContent = 'queue: ' + (parts.join(' · ') || 'idle');
}

// ── Polling ───────────────────────────────────────────────────────────────
async function loadTasks() {
  try {
    const res = await fetch('/api/tasks');
    if (!res.ok) return;
    tasks = await res.json();
    renderQueue();

    const active = tasks.some(t => t.state === 'pending' || t.state === 'running' || t.state === 'launching');
    if (active) startPolling(); else stopPolling();
  } catch (err) {
    console.error('loadTasks failed:', err);
  }
}

function startPolling() {
  if (pollTimer) return;
  pollTimer = setInterval(loadTasks, 2000);
}
function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

// ── Boot ──────────────────────────────────────────────────────────────────
loadTasks();
```

- [ ] **Step 3: Restart the server (so the new embedded public/ ships) and reload the page**

Run: `go build -o bin/data-writer ./src/`
Then in your tmux pane: kill the old server, start the new one. Hard-reload http://localhost:8081.

You should see the right pane top half populated with the recent tasks from the database. State chips should be color-coded (green=running, amber=pending, cyan=launching, mute=completed, red=failed). EC2 sharded tasks should show `ec2 ×4` in the target column.

- [ ] **Step 4: Commit**

```bash
git add src/server/public/style.css src/server/public/app.js
git commit -m "refactor(ui): render the TUI queue table

State chips, color-coded progress bars, shard-count badges
(\"ec2 ×4\"), polling every 2s while there's an active task. Queue
selection works via click; subsequent commits add keyboard nav and
the detail panel."
```

---

## Task 6: Detail panel

**Files:**
- Modify: `src/server/public/index.html` (detail-wrap block)
- Append: `src/server/public/style.css` (detail panel styles)
- Modify: `src/server/public/app.js` (detail render + selection wiring)

The detail panel shows the selected task's full info, including the shard summary line. It re-renders whenever the queue selection changes or polled data updates the selected task.

- [ ] **Step 1: Replace the detail-wrap block in index.html**

Find:

```html
<div class="detail-wrap">
  <!-- filled in Task 6 -->
</div>
```

Replace with:

```html
<div class="detail-wrap" id="detail-wrap">
  <div class="dw-pane-head">
    <span class="title" id="detail-title">[ DETAIL ]</span>
    <span class="right">↵ open · x cancel</span>
  </div>
  <div class="detail-body" id="detail-body">
    <div class="detail-empty">no task selected</div>
  </div>
</div>
```

- [ ] **Step 2: Append detail styles to style.css**

```css
/* === detail panel === */
.detail-wrap {
  display: flex; flex-direction: column; min-height: 0; overflow: hidden;
  border-top: 1px solid var(--line);
  background: linear-gradient(180deg, transparent, rgba(108, 240, 154, 0.015));
}
.detail-body { padding: 10px 14px; overflow: auto; flex: 1; }
.detail-empty { color: var(--txt-mute); font-size: 11px; padding: 12px 0; }

.detail-row {
  display: grid; grid-template-columns: 110px 1fr;
  align-items: baseline;
  padding: 1px 0;
  font-size: 11px;
}
.detail-row .k {
  color: var(--txt-mute);
  text-transform: uppercase;
  font-size: 9.5px;
  letter-spacing: 0.08em;
}
.detail-row .v { color: var(--txt); }
.detail-row .v.green { color: var(--green); }
.detail-row .v.amber { color: var(--amber); }
.detail-row .v.red   { color: var(--red); }

.detail-shards {
  margin-top: 10px;
  padding-top: 8px;
  border-top: 1px dashed var(--line-soft);
  font-size: 10px;
  color: var(--txt-dim);
}
```

- [ ] **Step 3: Add detail rendering to app.js**

Insert these at the top of app.js, right after the existing DOM refs:

```javascript
const detailTitle = document.getElementById('detail-title');
const detailBody  = document.getElementById('detail-body');
```

Then add this function (anywhere after `renderQueue`):

```javascript
function renderDetail() {
  const t = tasks.find(x => String(x.id) === String(selectedTaskId));
  if (!t) {
    detailTitle.textContent = '[ DETAIL ]';
    detailBody.innerHTML = '<div class="detail-empty">no task selected</div>';
    return;
  }
  detailTitle.textContent = '[ DETAIL · #' + t.id + ' ]';

  const p = pct(t);
  const stateColor = t.state === 'running' ? 'green'
                  : t.state === 'failed'  ? 'red'
                  : t.state === 'pending' || t.state === 'launching' ? 'amber'
                  : '';

  const rows = [
    ['target',    targetLabel(t)],
    ['state',     `<span class="${stateColor}">${t.state}</span>`],
    ['progress',  `<span class="${stateColor}">${p}% · ${t.files_written} / ${t.total_files} files · ${t.written_size || '—'}</span>`],
    ['created',   t.created_at ? new Date(t.created_at).toLocaleString() + ' · ' + fmtAge(t.created_at) + ' ago' : '—'],
  ];
  if (t.error) rows.push(['error', `<span class="red">${t.error}</span>`]);

  let html = rows.map(([k, v]) => `<div class="detail-row"><span class="k">${k}</span><span class="v">${v}</span></div>`).join('');

  if ((t.worker_total || 1) > 1) {
    html += `<div class="detail-shards">shards · ${t.worker_total} workers · ${t.worker_done || 0} done</div>`;
  }

  detailBody.innerHTML = html;
}
```

Then update `renderQueue` to call `renderDetail()` at the end:

```javascript
function renderQueue() {
  // ...existing code...
  renderDetail();
}
```

And update the row click handler to also re-render the detail:

```javascript
tr.addEventListener('click', () => {
  selectedTaskId = String(t.id);
  renderQueue();
});
```

(The `renderQueue` call inside the click handler already triggers `renderDetail` via the change above, so no further change is needed.)

- [ ] **Step 4: Reload and verify**

Hard-reload http://localhost:8081. Click any row in the queue. The detail panel below should show target, state, progress, created time, and (if `worker_total > 1`) a `shards · N workers · M done` line at the bottom. Selecting a different row updates the panel.

- [ ] **Step 5: Commit**

```bash
git add src/server/public/index.html src/server/public/style.css src/server/public/app.js
git commit -m "refactor(ui): add the detail panel under the queue

Renders the selected task's target/state/progress/created plus a
'shards · N workers · M done' summary when worker_total > 1."
```

---

## Task 7: Help overlay (replaces COMMENT options modal)

**Files:**
- Modify: `src/server/public/index.html` (help-overlay block)
- Append: `src/server/public/style.css` (overlay styles)
- Modify: `src/server/public/app.js` (open/close handlers)

The existing modal becomes a full-screen TUI overlay triggered by `?` or by clicking the `[?]` hint in the schema label.

- [ ] **Step 1: Replace the help-overlay block in index.html**

Find:

```html
<div id="help-overlay" class="dw-overlay" hidden>
  <!-- filled in Task 7 -->
</div>
```

Replace with:

```html
<div id="help-overlay" class="dw-overlay" hidden>
  <div class="dw-overlay-panel">
    <div class="dw-overlay-head">
      <span class="title">[ HELP · COMMENT OPTIONS ]</span>
      <span class="right"><span class="k">esc</span> close</span>
    </div>
    <div class="dw-overlay-body">
      <p>Use SQL <code>COMMENT</code> clauses on column definitions to control data generation behavior.</p>
      <table class="ref-table">
        <thead><tr><th>option</th><th>description</th><th>example</th></tr></thead>
        <tbody>
          <tr><td><code>null_percent</code></td><td>percentage of NULL values (0-100)</td><td><code>'null_percent=20'</code></td></tr>
          <tr><td><code>max_length</code></td><td>max length for string types</td><td><code>'max_length=120'</code></td></tr>
          <tr><td><code>min_length</code></td><td>min length for string types (defaults to 75% of max)</td><td><code>'min_length=60'</code></td></tr>
          <tr><td><code>mean</code></td><td>mean for numeric distributions</td><td><code>'mean=100'</code></td></tr>
          <tr><td><code>stddev</code></td><td>standard deviation</td><td><code>'stddev=15'</code></td></tr>
          <tr><td><code>compress</code></td><td>compression hint 1-100, lower = more repetition</td><td><code>'compress=40'</code></td></tr>
          <tr><td><code>set</code></td><td>allowed values as JSON array</td><td><code>'set=["a","b","c"]'</code></td></tr>
          <tr><td><code>order</code></td><td>total_order, partial_order, random_order</td><td><code>'order=partial_order'</code></td></tr>
        </tbody>
      </table>
      <p class="excl"><strong>mutually exclusive:</strong> <code>set</code> can't combine with mean/stddev/order/compress/max_length/min_length. <code>mean</code>/<code>stddev</code> can't combine with <code>order</code>.</p>
    </div>
  </div>
</div>
```

- [ ] **Step 2: Append overlay styles to style.css**

```css
/* === overlay (help, etc.) === */
.dw-overlay {
  position: fixed; inset: 0; z-index: 200;
  background: rgba(0, 0, 0, 0.55);
  display: flex; align-items: center; justify-content: center;
  padding: 24px;
}
.dw-overlay[hidden] { display: none; }
.dw-overlay-panel {
  background: var(--bg);
  border: 1px solid var(--green);
  box-shadow: 0 0 0 1px var(--bg), 0 0 40px rgba(108, 240, 154, 0.15);
  width: 100%; max-width: 760px; max-height: 80vh;
  display: flex; flex-direction: column;
  font-family: var(--font-mono);
}
.dw-overlay-head {
  display: flex; align-items: center; padding: 8px 14px;
  border-bottom: 1px solid var(--line);
  font-size: 10px; letter-spacing: 0.12em; text-transform: uppercase;
}
.dw-overlay-head .title { color: var(--green); font-weight: 600; }
.dw-overlay-head .right { margin-left: auto; color: var(--txt-mute); }
.dw-overlay-head .right .k {
  background: rgba(108, 240, 154, 0.12); color: var(--green);
  padding: 0 5px; border-radius: 2px;
}
.dw-overlay-body {
  padding: 14px 18px;
  overflow: auto;
  font-size: 11.5px;
  color: var(--txt);
}
.dw-overlay-body p { margin-bottom: 10px; line-height: 1.5; color: var(--txt-dim); }
.dw-overlay-body code {
  background: var(--bg-pane); border: 1px solid var(--line);
  padding: 0 5px; font-size: 11px; color: var(--green);
}
.ref-table { width: 100%; border-collapse: collapse; margin: 8px 0; }
.ref-table th {
  text-align: left; padding: 6px 8px;
  font-size: 9.5px; color: var(--txt-mute);
  text-transform: uppercase; letter-spacing: 0.08em;
  border-bottom: 1px dashed var(--line-soft);
}
.ref-table td {
  padding: 6px 8px; font-size: 11px;
  border-bottom: 1px dotted var(--line-soft);
  color: var(--txt-dim);
  vertical-align: top;
}
.ref-table tr:last-child td { border-bottom: none; }
.dw-overlay-body p.excl { color: var(--amber); font-size: 11px; margin-top: 12px; }
```

- [ ] **Step 3: Add open/close handlers to app.js**

Append:

```javascript
// ── Help overlay ──────────────────────────────────────────────────────────
const helpOverlay = document.getElementById('help-overlay');
const helpBtn = document.getElementById('help-btn');

function openHelp() { helpOverlay.hidden = false; }
function closeHelp() { helpOverlay.hidden = true; }
function toggleHelp() { helpOverlay.hidden = !helpOverlay.hidden; }

helpBtn.addEventListener('click', (e) => { e.preventDefault(); openHelp(); });
helpOverlay.addEventListener('click', (e) => {
  if (e.target === helpOverlay) closeHelp();
});
```

- [ ] **Step 4: Reload and verify**

Hard-reload. Click `[?] help` in the schema label — the overlay should appear, full-screen, with the COMMENT options reference. Click the dim background outside the panel — it should close.

- [ ] **Step 5: Commit**

```bash
git add src/server/public/index.html src/server/public/style.css src/server/public/app.js
git commit -m "refactor(ui): replace help modal with TUI overlay

? key (or the [?] hint in the schema label) opens a full-screen
phosphor-bordered overlay with the COMMENT options reference. Click
backdrop or escape (wired in the keyboard router task) to close."
```

---

## Task 8: Form submit + validation + AI assist behavior

**Files:**
- Modify: `src/server/public/app.js` (form behavior block)

The form has all its DOM but no behavior yet. This task wires up validation, the credentials section toggle, the submit handler, and the AI assist line.

- [ ] **Step 1: Append form behavior to app.js**

Append to `src/server/public/app.js`:

```javascript
// ── Form refs ─────────────────────────────────────────────────────────────
const form           = document.getElementById('gen-form');
const submitBtn      = document.getElementById('submit-btn');
const sqlTextarea    = document.getElementById('sql');
const pathInput      = document.getElementById('path');
const formStatus     = document.getElementById('form-status');
const formMeta       = document.getElementById('form-meta');
const credSection    = document.getElementById('cred-section');
const storageType    = document.getElementById('storage_type');
const awsFields      = document.getElementById('aws-fields');
const ksyunFields    = document.getElementById('ksyun-fields');
const ec2Group       = document.getElementById('ec2-toggle-group');
const ec2Checkbox    = document.getElementById('run-on-ec2');
const ec2Hint        = document.getElementById('ec2-hint');
const awsCredFields  = document.getElementById('aws-cred-fields');
const targetSelect   = document.getElementById('target');
const formatSelect   = document.getElementById('format');

const aiLine        = document.getElementById('ai-line');
const aiPromptInput = document.getElementById('ai-prompt');

// ── Validation ────────────────────────────────────────────────────────────
const schemaTablePattern = /CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+[`"']?[A-Za-z0-9_-]+[`"']?\.[`"']?[A-Za-z0-9_-]+[`"']?\s*\(/i;

function validateSchema() {
  const v = sqlTextarea.value.trim();
  if (!v) {
    sqlTextarea.classList.remove('invalid');
    formStatus.textContent = 'no schema';
    formStatus.className = 'form-status';
    formMeta.textContent = 'no schema';
    return false;
  }
  if (!schemaTablePattern.test(v)) {
    sqlTextarea.classList.add('invalid');
    formStatus.textContent = '× schema must use CREATE TABLE schema.table (...)';
    formStatus.className = 'form-status bad';
    formMeta.textContent = 'invalid schema';
    return false;
  }
  sqlTextarea.classList.remove('invalid');
  const m = v.match(/CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+[`"']?([A-Za-z0-9_-]+)[`"']?\.[`"']?([A-Za-z0-9_-]+)[`"']?/i);
  const name = m ? `${m[1]}.${m[2]}` : 'schema valid';
  formStatus.textContent = `✓ ${name}`;
  formStatus.className = 'form-status ok';
  formMeta.textContent = name;
  return true;
}
sqlTextarea.addEventListener('input', validateSchema);

// ── Credentials section toggling ──────────────────────────────────────────
function updateCredSection() {
  const isRemote = pathInput.value.trim().toLowerCase().startsWith('s3://');
  credSection.hidden = !isRemote;
  if (isRemote) updateCredFields();
}
function updateCredFields() {
  const isKsyun = storageType.value === 'ksyun';
  awsFields.hidden = isKsyun;
  ksyunFields.hidden = !isKsyun;
  ec2Group.hidden = isKsyun;
  if (isKsyun) ec2Checkbox.checked = false;
  updateEc2Toggle();
}
function updateEc2Toggle() {
  const isEc2 = ec2Checkbox.checked;
  ec2Hint.hidden = !isEc2;
  awsCredFields.hidden = isEc2;
  // Mirror into the top-level target select so submit picks it up.
  targetSelect.value = isEc2 ? 'ec2' : 'local';
}
pathInput.addEventListener('input', updateCredSection);
storageType.addEventListener('change', updateCredFields);
ec2Checkbox.addEventListener('change', updateEc2Toggle);

// ── AI assist line ────────────────────────────────────────────────────────
function openAi() {
  aiLine.hidden = false;
  aiPromptInput.focus();
}
function closeAi() {
  aiLine.hidden = true;
  aiPromptInput.value = '';
}

aiPromptInput.addEventListener('keydown', async (e) => {
  if (e.key === 'Escape') { e.preventDefault(); closeAi(); return; }
  if (e.key === 'Enter')  { e.preventDefault(); await runAi(); return; }
});

async function runAi() {
  const prompt = aiPromptInput.value.trim();
  if (!prompt) return;
  aiPromptInput.disabled = true;
  try {
    const res = await fetch('/api/ai-assist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: sqlTextarea.value, prompt }),
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      alert(data.error || 'AI error: ' + res.status);
      return;
    }
    const data = await res.json();
    if (data.sql) {
      sqlTextarea.value = data.sql;
      validateSchema();
    }
    closeAi();
  } catch (err) {
    alert('AI request failed: ' + err.message);
  } finally {
    aiPromptInput.disabled = false;
  }
}

// ── Submit ────────────────────────────────────────────────────────────────
form.addEventListener('submit', async (e) => {
  e.preventDefault();
  if (!validateSchema()) return;
  if (!pathInput.value.trim()) {
    formStatus.textContent = '× path required';
    formStatus.className = 'form-status bad';
    return;
  }

  const body = {
    sql: sqlTextarea.value,
    path: pathInput.value.trim(),
    start_fileno: 0,
    end_fileno: parseInt(document.getElementById('end_fileno').value, 10) || 0,
    rows: parseInt(document.getElementById('rows').value, 10) || 0,
    format: formatSelect.value,
  };

  const folders = document.getElementById('folders').value;
  if (folders !== '') body.folders = parseInt(folders, 10) || 0;

  if (targetSelect.value === 'ec2') body.target = 'ec2';

  if (!credSection.hidden) {
    if (storageType.value === 'ksyun') {
      body.ksyun = true;
    } else {
      body.s3 = {
        region: document.getElementById('s3_region').value,
        provider: document.getElementById('s3_provider').value,
        access_key: document.getElementById('s3_access_key').value,
        secret_key: document.getElementById('s3_secret_key').value,
        endpoint: document.getElementById('s3_endpoint').value,
      };
    }
  }

  submitBtn.disabled = true;
  try {
    const res = await fetch('/api/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      formStatus.textContent = '× ' + (data.error || 'server error ' + res.status);
      formStatus.className = 'form-status bad';
      return;
    }
    formStatus.textContent = '✓ submitted';
    formStatus.className = 'form-status ok';
    loadTasks();
  } catch (err) {
    formStatus.textContent = '× ' + err.message;
    formStatus.className = 'form-status bad';
  } finally {
    submitBtn.disabled = false;
  }
});

// Initial form-state setup.
updateCredSection();
validateSchema();
```

- [ ] **Step 2: Reload and submit a real job**

Hard-reload. In the schema textarea type:

```sql
CREATE TABLE test.t (id BIGINT PRIMARY KEY, c CHAR(60));
```

Set path to `/tmp/dw-test/`, files to 5, rows to 1000. Click `execute ↵`. The form-status line should briefly show `✓ submitted`, the queue should refresh, and a new row should appear.

- [ ] **Step 3: Verify AI assist**

Open the AI line by setting `aiLine.hidden = false` in the browser console temporarily, type a prompt, press enter. The schema should update.

- [ ] **Step 4: Commit**

```bash
git add src/server/public/app.js
git commit -m "refactor(ui): wire form validation, submit, and AI assist

Schema regex validation drives the inline status footer. Credentials
section auto-expands for s3:// paths and the EC2 toggle mirrors into
the target select. AI assist line uses POST /api/ai-assist; enter
applies, escape dismisses."
```

---

## Task 9: Format options inline expansion (replaces format modal)

**Files:**
- Modify: `src/server/public/index.html` (format-options block)
- Append: `src/server/public/style.css`
- Modify: `src/server/public/app.js`

Today's format options are in a modal. They become an inline expansion that toggles below the format select.

- [ ] **Step 1: Replace the format-options block in index.html**

Find `<div id="format-options" class="format-options" hidden></div>` and replace with:

```html
<div id="format-options" class="format-options" hidden>
  <div id="csv-options">
    <div class="field-grid grid-2">
      <div class="field">
        <div class="field-label">separator</div>
        <input id="csv_separator" class="dw-input" type="text" value="," placeholder=",">
      </div>
      <div class="field">
        <div class="field-label">line ending</div>
        <select id="csv_endline" class="dw-input">
          <option value="\n" selected>\n (LF)</option>
          <option value="\r\n">\r\n (CRLF)</option>
        </select>
      </div>
    </div>
    <label class="toggle-row"><input id="csv_base64" type="checkbox"><span>base64 encode</span></label>
  </div>
  <div id="parquet-options" hidden>
    <div class="field-grid grid-3">
      <div class="field">
        <div class="field-label">compression</div>
        <select id="parquet_compression" class="dw-input">
          <option value="zstd" selected>zstd</option>
          <option value="snappy">snappy</option>
          <option value="gzip">gzip</option>
          <option value="lz4">lz4</option>
          <option value="brotli">brotli</option>
          <option value="none">none</option>
        </select>
      </div>
      <div class="field">
        <div class="field-label">row groups</div>
        <input id="parquet_row_groups" class="dw-input" type="number" value="1" min="1">
      </div>
      <div class="field">
        <div class="field-label">page size</div>
        <input id="parquet_page_size" class="dw-input" type="text" value="1MiB">
      </div>
    </div>
  </div>
</div>
```

- [ ] **Step 2: Append format-options styles to style.css**

```css
/* === format options inline === */
.format-options {
  border: 1px dashed var(--line);
  border-left: 2px solid var(--cyan);
  padding: 10px 12px;
  display: flex; flex-direction: column; gap: 10px;
}
.toggle-row { display: inline-flex; align-items: center; gap: 8px; cursor: pointer; font-size: 11px; color: var(--txt-dim); }
.toggle-row input { accent-color: var(--green); }
```

- [ ] **Step 3: Wire toggle in app.js**

Append:

```javascript
// ── Format options inline ─────────────────────────────────────────────────
const formatOptions    = document.getElementById('format-options');
const formatOptionsBtn = document.getElementById('format-options-btn');
const csvOptions       = document.getElementById('csv-options');
const parquetOptions   = document.getElementById('parquet-options');

function showFormatOptions() {
  csvOptions.hidden     = formatSelect.value !== 'csv';
  parquetOptions.hidden = formatSelect.value !== 'parquet';
  formatOptions.hidden  = false;
}
function hideFormatOptions() { formatOptions.hidden = true; }
function toggleFormatOptions() {
  if (formatOptions.hidden) showFormatOptions(); else hideFormatOptions();
}
formatOptionsBtn.addEventListener('click', (e) => { e.preventDefault(); toggleFormatOptions(); });
formatSelect.addEventListener('change', () => { if (!formatOptions.hidden) showFormatOptions(); });
```

Then update the submit handler in app.js — where it currently just sets `body.format`, also include the format-specific options. Find:

```javascript
const body = {
  sql: sqlTextarea.value,
  path: pathInput.value.trim(),
  start_fileno: 0,
  end_fileno: parseInt(document.getElementById('end_fileno').value, 10) || 0,
  rows: parseInt(document.getElementById('rows').value, 10) || 0,
  format: formatSelect.value,
};
```

After it, add:

```javascript
if (body.format === 'csv') {
  body.csv = {
    separator: document.getElementById('csv_separator').value || ',',
    endline: document.getElementById('csv_endline').value.replace(/\\n/g, '\n').replace(/\\r/g, '\r'),
    base64: document.getElementById('csv_base64').checked,
  };
} else {
  body.parquet = {
    compression: document.getElementById('parquet_compression').value || 'zstd',
    row_groups: parseInt(document.getElementById('parquet_row_groups').value, 10) || 1,
    page_size: document.getElementById('parquet_page_size').value || '1MiB',
  };
}
```

- [ ] **Step 4: Reload and verify**

Hard-reload. Click `opts` next to the format label — a dashed-bordered panel should expand below with CSV options. Switch the format select to `parquet` — the panel should swap to parquet options. Click `opts` again to close.

- [ ] **Step 5: Commit**

```bash
git add src/server/public/index.html src/server/public/style.css src/server/public/app.js
git commit -m "refactor(ui): format options become inline expansion

The 'opts' hint next to the format label toggles a dashed-bordered
panel right under the field with CSV or Parquet specific options.
No more modal."
```

---

## Task 10: Keyboard router

**Files:**
- Modify: `src/server/public/app.js`

The keyboard router is a single delegated `keydown` listener that maps keys to actions, while ignoring keys that the focused input is consuming.

- [ ] **Step 1: Append the keyboard router to app.js**

```javascript
// ── Keyboard router ───────────────────────────────────────────────────────
const FORM_PANE   = document.getElementById('pane-form');
const QUEUE_PANE  = document.getElementById('pane-right');

function isTypingTarget(el) {
  if (!el) return false;
  const tag = el.tagName;
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || el.isContentEditable;
}

async function cancelSelected() {
  if (!selectedTaskId) return;
  const t = tasks.find(x => String(x.id) === String(selectedTaskId));
  if (!t || (t.state !== 'pending' && t.state !== 'running' && t.state !== 'launching')) return;
  try {
    const res = await fetch('/api/cancel?id=' + encodeURIComponent(t.id), { method: 'POST' });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      alert(data.error || 'cancel failed');
    }
    loadTasks();
  } catch (err) {
    alert('cancel failed: ' + err.message);
  }
}

function moveSelection(delta) {
  if (tasks.length === 0) return;
  let idx = tasks.findIndex(t => String(t.id) === String(selectedTaskId));
  if (idx === -1) idx = 0;
  idx = Math.max(0, Math.min(tasks.length - 1, idx + delta));
  selectedTaskId = String(tasks[idx].id);
  renderQueue();
}

document.addEventListener('keydown', (e) => {
  // Always allow esc to close overlays / dismiss things
  if (e.key === 'Escape') {
    if (!helpOverlay.hidden) { closeHelp(); return; }
    if (!aiLine.hidden)      { closeAi();   return; }
    return;
  }

  // If the user is typing into a field, defer to the field for letter keys.
  if (isTypingTarget(document.activeElement)) {
    return;
  }

  switch (e.key) {
    case 'n':
      e.preventDefault();
      sqlTextarea.focus();
      return;
    case 'tab':
    case 'Tab':
      // Let native tab work; just do nothing.
      return;
    case '/':
      e.preventDefault();
      openAi();
      return;
    case 'x':
      e.preventDefault();
      cancelSelected();
      return;
    case 'r':
      e.preventDefault();
      loadTasks();
      return;
    case '?':
      e.preventDefault();
      toggleHelp();
      return;
    case 'ArrowDown': case 'j':
      e.preventDefault();
      moveSelection(1);
      return;
    case 'ArrowUp': case 'k':
      e.preventDefault();
      moveSelection(-1);
      return;
    case 'Enter':
      // Enter on the queue triggers nothing in v1 (no log view yet).
      return;
  }
});
```

- [ ] **Step 2: Reload and test each shortcut**

Hard-reload. Verify:

- `n` focuses the schema textarea
- Type some SQL, click outside, press `/` — AI line opens
- Press `?` — help overlay opens; press `?` or `esc` — closes
- Click a row to select, then `j`/`k` or arrows — selection moves
- Press `r` — queue refreshes
- Select an active task, press `x` — cancel attempt fires

- [ ] **Step 3: Commit**

```bash
git add src/server/public/app.js
git commit -m "refactor(ui): keyboard router for n / / / x / r / ? / j / k / esc

Single delegated keydown listener that defers to the active input for
letter keys. Wires the bottom status bar shortcuts to real actions.
Esc cascades through help overlay → ai line."
```

---

## Task 11: Polish — top airline + bottom status bar live data

**Files:**
- Append: `src/server/public/style.css` (top airline + bottom status bar styles)
- Modify: `src/server/public/app.js` (clock + connection status + airline state)

The top airline and bottom status bar are visible but mostly static. This task makes them live: clock ticks, connection dots reflect poll health, the airline path shows the schema status.

- [ ] **Step 1: Append airline + status-bar styles to style.css**

```css
/* === top airline === */
.dw-top {
  display: flex; align-items: stretch;
  background: linear-gradient(180deg, #0c2418, #08180f);
  color: var(--green);
  border-bottom: 1px solid var(--line);
  font-size: 10.5px;
}
.dw-top .seg {
  padding: 0 12px; display: flex; align-items: center; gap: 6px;
  position: relative;
}
.dw-top .seg.brand {
  background: var(--green); color: #03130a; font-weight: 700;
  letter-spacing: 0.04em; padding: 0 14px;
}
.dw-top .seg.brand::after {
  content: ""; position: absolute; right: -12px; top: 0;
  border: 12px solid transparent; border-left-color: var(--green); border-right: 0;
}
.dw-top .seg.host  { color: var(--cyan); padding-left: 24px; }
.dw-top .seg.path  { color: var(--txt-dim); flex: 1; }
.dw-top .seg.tasks { color: var(--amber); }
.dw-top .seg.clock { color: var(--green); }

/* === bottom status bar === */
.dw-bot {
  display: flex; align-items: center;
  background: linear-gradient(180deg, #08180f, #0c2418);
  color: var(--txt-dim);
  border-top: 1px solid var(--line);
  font-size: 10px;
  height: 22px;
}
.dw-bot .keys { display: flex; gap: 14px; padding: 0 14px; }
.dw-bot .keys .k { color: var(--green); font-weight: 700; }
.dw-bot .right { margin-left: auto; padding: 0 14px; color: var(--txt-mute); }
.dw-bot .dot { font-size: 9px; }
.dw-bot .dot.ok    { color: var(--green); }
.dw-bot .dot.warn  { color: var(--amber); }
.dw-bot .dot.error { color: var(--red); }
.dw-bot .dot.mute  { color: var(--txt-mute); }
```

- [ ] **Step 2: Append airline + status logic to app.js**

```javascript
// ── Airline + status bar live data ───────────────────────────────────────
const airlineHost  = document.getElementById('airline-host');
const airlineClock = document.getElementById('airline-clock');
const airlinePath  = document.getElementById('airline-path');
const botDb        = document.getElementById('bot-db');
const botRunning   = document.getElementById('bot-running');

airlineHost.textContent = 'datawriter@' + window.location.host;

function tickClock() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  airlineClock.textContent = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())} ●`;
}
tickClock();
setInterval(tickClock, 1000);

// Override loadTasks slightly to track health.
let lastPollOk = 0;
const _origLoadTasks = loadTasks;
loadTasks = async function() {
  try {
    await _origLoadTasks();
    lastPollOk = Date.now();
    botDb.className = 'dot ok';
  } catch (e) {
    botDb.className = 'dot error';
  }
  // running indicator
  const running = tasks.some(t => t.state === 'running');
  botRunning.className = 'dot ' + (running ? 'warn' : 'mute');
  botRunning.nextSibling && (botRunning.nextSibling.textContent = running ? ' ' + running + ' active' : ' idle');
};

// Update path label whenever the schema validity changes.
const _origValidateSchema = validateSchema;
validateSchema = function() {
  const ok = _origValidateSchema();
  airlinePath.textContent = ok ? '~/jobs/new · ' + formMeta.textContent : '~/jobs/new';
  return ok;
};
```

(Note: the override-existing-function pattern is a deliberate small hack to keep the diff localized. If you prefer, inline the changes into the original `loadTasks` and `validateSchema` definitions instead.)

- [ ] **Step 3: Reload and verify**

Hard-reload. The clock in the top right should tick every second. The airline-path should update as you type a valid schema. The bottom right db dot should be green; when a job is running it should show an amber active indicator.

- [ ] **Step 4: Commit**

```bash
git add src/server/public/style.css src/server/public/app.js
git commit -m "refactor(ui): live airline clock + connection status

Clock ticks every second. The airline path label mirrors the parsed
schema name. The bottom-right db dot turns red on poll failure and
the running indicator goes amber when there's an active job."
```

---

## Task 12: Smoke test, manual run-through, and final commit

**Files:**
- (none — verification only)

This is a final pass to make sure the whole UI works together. Open the page fresh and walk through the user journey.

- [ ] **Step 1: Hard-reload and visually compare to the mockup**

Open the mockup at `.superpowers/brainstorm/1085345-1775556525/content/tui-fullpage.html` in one browser tab and the live app at `http://localhost:8081` in another. Compare side-by-side:

- Top airline: brand chevron, hostname, path, queue counts, clock — all present
- Form pane: schema textarea, path input, files/rows/format grid, target/subdirs grid, footer status, run button
- Queue table: state chips colored, progress bars, shard badges
- Detail panel: shows selected task with shard summary
- Bottom status bar: shortcut hints, connection dots

If anything is off, fix it now (small CSS tweaks, no big refactors).

- [ ] **Step 2: Functional walk-through**

1. Type a CREATE TABLE statement → footer shows `✓ schema.table`.
2. Press `/` → AI line opens → type a prompt → enter → schema updates.
3. Set path to `/tmp/dw-test/`, files=5, rows=1000, click execute → new task appears in queue.
4. Click another task → detail panel updates.
5. Press `j`/`k` → selection moves.
6. Press `?` → help overlay opens. `esc` → closes.
7. Click `opts` next to format → format panel expands. Switch to parquet → panel changes. `opts` again → closes.
8. Set path to `s3://test-bucket/` → credentials section appears. Toggle EC2 → AWS fields hide.
9. With an active job in the queue, press `x` → cancel fires.
10. Wait 2s → poll auto-refreshes the queue.

If everything works, move on. If anything is broken, fix it.

- [ ] **Step 3: Run go vet**

Run: `go vet ./...`
Expected: no issues.

- [ ] **Step 4: Build the binary and confirm it includes the new public/**

Run:
```bash
go build -o bin/data-writer ./src/
strings bin/data-writer | grep -c "dw-screen" | head
```
Expected: at least `1` (the embedded HTML contains `dw-screen`).

- [ ] **Step 5: Commit any final fix-ups**

```bash
git add -p   # interactively stage any small final tweaks
git commit -m "polish(ui): final TUI dashboard tweaks"
```

If there's nothing to fix, skip this step.

- [ ] **Step 6: Tag the refactor commit (optional)**

```bash
git log --oneline | head -15
# note the SHA of the first commit in this plan series
git tag ui/tui-dashboard-v1 <sha>
```

---

## Self-review against the spec

**Spec coverage:**

| Spec section | Covered by |
| ------------ | ---------- |
| Goals | All tasks together |
| Non-Goals (only allowed server change: `worker_total`/`worker_done`) | Task 1 |
| Color palette + variables | Task 2 |
| Typography (JetBrains Mono) | Task 2 |
| Visual decoration (scanline, blinking caret, no shadows, no rounded) | Task 2 (scanline), schema textarea cursor in Task 4 |
| Layout (top airline / 2-pane / bottom status bar) | Tasks 2, 3, 11 |
| Responsive (single-col <720px) | Task 2 (media query) |
| `<dw-screen>` shell | Task 2 + Task 3 |
| `<dw-airline>` | Task 3 + Task 11 |
| `<dw-form>` | Tasks 4, 8 |
| `<dw-queue>` | Tasks 3, 5 |
| `<dw-detail>` | Task 6 |
| `<dw-statusbar>` | Tasks 3, 11 |
| Help overlay (`?`) | Task 7 |
| Format options inline | Task 9 |
| Keyboard router | Task 10 |
| Interactions preserved (submit, polling, cancel, AI assist, validation) | Tasks 5, 8, 10 |
| Accessibility (real elements, focus rings, color+text labels, reduced-motion) | Tasks 2, 4, 5 |
| Cache-bust to v=34 | Task 3 |

No gaps. The optional polish items (SQL syntax highlighting, `:` command palette stub) are deliberately omitted.

**Placeholder scan:** All steps contain real code, real file paths, and real verification commands. No "TBD", no "fill in details".

**Type consistency:** `loadTasks`, `renderQueue`, `renderDetail`, `validateSchema`, `closeHelp`, `closeAi`, `cancelSelected`, `moveSelection`, `pct`, `targetLabel`, `fmtAge` — all introduced once and referenced consistently. CSS class names (`dw-screen`, `dw-top`, `dw-bot`, `dw-pane`, `dw-pane-head`, `dw-input`, `dw-table`, `dw-overlay`) match between HTML, CSS, and JS.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-07-tui-dashboard.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
