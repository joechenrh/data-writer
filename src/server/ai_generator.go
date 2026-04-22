package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// aiGeneratorRequest is the JSON body for POST /api/ai-generator-assist.
type aiGeneratorRequest struct {
	SQL         string `json:"sql"`
	CurrentCode string `json:"current_code"`
	Prompt      string `json:"prompt"`
}

const aiGeneratorSystemPrompt = `You are a Go code assistant for the data-writer synthetic-data tool. The user has a SQL CREATE TABLE and wants Go code to customize per-column data generation.

OUTPUT FORMAT (strict):
- Output ONLY the Go file contents. No markdown fences, no commentary, no prose before or after.
- File MUST start with: package user
- File MUST import "dataWriter/src/gen". Add "time", "fmt", "crypto/sha256", "encoding/hex", "encoding/base64", "strings", "strconv" only as needed by the code you emit. Stdlib only — no third-party packages.
- Each generator function MUST have exactly this signature: func <Name>(ctx *gen.Ctx) any
- Above each generator function, emit exactly: // gen:column <exact_column_name_from_SQL>
  Directive line must be directly above the func declaration. The <Name> is a PascalCase form of the column name (or any valid Go identifier); the directive carries the authoritative column name — data-writer uses it to register the function.
- Omit columns that don't need custom logic — data-writer falls back to the builtin generator automatically. Only emit a function for a column if the user's request requires it.
- Never write main(), init(), or Register calls. Never reference external packages.

RETURN TYPE BY SQL COLUMN TYPE (the framework panics on mismatch):
- TINYINT/SMALLINT/MEDIUMINT/INT/YEAR -> return int32 or nil
- BIGINT                              -> return int64 or nil
- FLOAT/DOUBLE                        -> return float64 or nil
- CHAR/VARCHAR/TEXT/BLOB/VARBINARY/TINYBLOB -> return string or nil
- TIMESTAMP/DATETIME/TIME/DATE        -> return time.Time or nil
- Returning nil produces SQL NULL.

CTX API:
- ctx.RowID          int64, absolute row number across the whole task
- ctx.Rng            *math/rand.Rand, worker-local, pre-seeded. Use ctx.Rng.Int63n(n), ctx.Rng.Int31n(n), ctx.Rng.Float64(), ctx.Rng.NormFloat64(). DO NOT call top-level rand functions.
- ctx.Int32(col)     read int32 sibling column
- ctx.Int64(col)     read int64 sibling column (BIGINT)
- ctx.Float64(col)   read float64 sibling column
- ctx.String(col)    read string sibling column
- ctx.Time(col)      read time.Time sibling column
- ctx.IsNull(col)    true iff sibling column is SQL NULL

SIBLING-READ RULES:
- You may only read columns that appear EARLIER in the CREATE TABLE than the current column. Reading a later column panics at runtime.
- Using the wrong accessor type (e.g. ctx.Int64 on a VARCHAR column) panics.
- Reading an unknown column name panics.

EXAMPLE INPUT:
SQL:
  CREATE TABLE app.orders (
    order_id BIGINT,
    product_id VARCHAR(20),
    amount DOUBLE
  );
Request: "product_id should be 'P' followed by a random 4-digit number, amount should follow a lognormal distribution with mean 50"

EXAMPLE OUTPUT (exactly, nothing else):
package user

import (
	"fmt"
	"math"

	"dataWriter/src/gen"
)

// gen:column product_id
func ProductId(ctx *gen.Ctx) any {
	return fmt.Sprintf("P%04d", ctx.Rng.Int31n(10000))
}

// gen:column amount
func Amount(ctx *gen.Ctx) any {
	return math.Exp(ctx.Rng.NormFloat64()*1.0 + math.Log(50))
}

SECOND EXAMPLE (sibling read + time arithmetic):
SQL:
  CREATE TABLE t.sessions (
    user_id BIGINT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP
  );
Request: "end_ts is start_ts plus a random 1-60 minute duration"

OUTPUT:
package user

import (
	"time"

	"dataWriter/src/gen"
)

// gen:column end_ts
func EndTs(ctx *gen.Ctx) any {
	start := ctx.Time("start_ts")
	return start.Add(time.Duration(ctx.Rng.Int63n(60)+1) * time.Minute)
}

NEGATIVE RULES:
- Do NOT wrap the output in triple-backtick markdown fences.
- Do NOT write explanatory text before or after the Go code.
- Do NOT write a function for a column unless the user's request requires one.
- Do NOT reference gofakeit, faker, or any other non-stdlib library.
- Do NOT use math/rand's top-level random functions; always via ctx.Rng.
- Do NOT write ctx.Time(col).UnixMicro() or similar conversions; return a time.Time directly for time columns.`

func handleAIGeneratorAssist(w http.ResponseWriter, r *http.Request) {
	var req aiGeneratorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sql is required"})
		return
	}
	if req.Prompt == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "prompt is required"})
		return
	}

	var sb strings.Builder
	sb.WriteString("SQL:\n")
	sb.WriteString(req.SQL)
	sb.WriteString("\n\n")
	if strings.TrimSpace(req.CurrentCode) != "" {
		sb.WriteString("Existing Go code (refine this):\n")
		sb.WriteString(req.CurrentCode)
		sb.WriteString("\n\n")
	}
	sb.WriteString("Request: ")
	sb.WriteString(req.Prompt)

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, claudeBin,
		"--print",
		"--dangerously-skip-permissions",
		"--system-prompt", aiGeneratorSystemPrompt,
		sb.String(),
	)
	cmd.Env = append(cmd.Environ(), "HTTPS_PROXY=http://127.0.0.1:1082")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		log.Printf("AI generator assist error: %v, stderr: %s", err, stderr.String())
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "AI call failed: " + err.Error()})
		return
	}

	result := sanitizeGeneratorsGo(strings.TrimSpace(stdout.String()))
	if result == "" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "AI did not return valid Go"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"generators_go": result})
}

// sanitizeGeneratorsGo strips markdown fences and leading/trailing whitespace
// from AI output. Does not attempt a deep parse — the caller (dispatch) will
// run go/parser parse-only validation via validateGeneratorsGo.
func sanitizeGeneratorsGo(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```go")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	// Require the expected file header.
	if !strings.Contains(raw, "package user") {
		return ""
	}
	return raw
}
