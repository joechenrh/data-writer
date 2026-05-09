package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var createTableRe = regexp.MustCompile(`(?is)(CREATE\s+TABLE\s+.+?;)`)

// aiAssistRequest is the JSON body for POST /api/ai-assist.
type aiAssistRequest struct {
	SQL    string `json:"sql"`
	Prompt string `json:"prompt"`
}

const aiSystemPrompt = `You are an SQL schema assistant for a data generation tool.
Given a CREATE TABLE statement and a user request, modify the SQL to match the request.
If the user provides no existing SQL, create a new CREATE TABLE statement.

CRITICAL: The table name MUST be qualified with a schema name in the form "CREATE TABLE schema_name.table_name (...)".
Pick a reasonable schema name based on context (e.g. "test", "app", "db", or domain-specific like "users", "ecommerce").
Never output an unqualified "CREATE TABLE table_name (...)" - this will be rejected.

The tool supports these COMMENT options on column definitions to control data generation:
- null_percent=N: Percentage of NULL values (0-100)
- max_length=N: Max length for string types (CHAR, VARCHAR, TEXT)
- min_length=N: Min length for string types, defaults to 75%% of max_length
- mean=N: Mean for numeric distributions (INT, BIGINT, FLOAT, etc.)
- stddev=N: Standard deviation for numeric distributions
- compress=N: Compression ratio hint (1-100), lower = more repeated values
- set=[...]: Allowed values as JSON array, e.g. set=["a","b"] or set=[1,2,3]
- order=total_order|partial_order|random_order: Integer ordering (total_order=strictly increasing, partial_order=mostly increasing, random_order=default)

Multiple options can be combined in one COMMENT, separated by commas, e.g.: COMMENT 'mean=100, stddev=15'

MUTUALLY EXCLUSIVE OPTIONS (will be rejected if combined):
- 'set' cannot be combined with mean, stddev, order, compress, max_length, or min_length (only null_percent is allowed alongside set).
- 'mean'/'stddev' cannot be combined with 'order' (a normal distribution and an enforced ordering can't coexist).

DEFAULT PREFERENCES (apply unless the user explicitly asks otherwise):
- Prefer BIGINT over INT/SMALLINT/TINYINT for integer columns.
- Prefer NOT to emit COMMENT options. The ONLY allowed defaults are:
  * max_length / min_length on string columns when a length constraint is needed.
  * order=random_order on integer columns that are NOT part of the primary key.
  Omit null_percent, mean, stddev, compress, and set unless the user explicitly requests them.
- Multi-column PRIMARY KEY is NOT currently supported. Always use a single-column PRIMARY KEY.

CRITICAL RULES:
- Your ENTIRE response must be a valid SQL statement starting with CREATE TABLE.
- NEVER include explanations, apologies, markdown fences, or any text before/after the SQL.
- If the request is unclear, still return a valid CREATE TABLE statement with your best guess.
- Do NOT say "I cannot", "The request", "Here is", etc. Just output SQL.`

const claudeBin = "/mnt/data/joechenrh/.local/bin/claude"

func handleAIAssist(w http.ResponseWriter, r *http.Request) {
	var req aiAssistRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.Prompt == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "prompt is required"})
		return
	}

	userMsg := req.Prompt
	if req.SQL != "" {
		userMsg = "Current SQL:\n" + req.SQL + "\n\nRequest: " + req.Prompt
	}

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, claudeBin,
		"--print",
		"--dangerously-skip-permissions",
		"--system-prompt", aiSystemPrompt,
		userMsg,
	)
	cmd.Env = append(cmd.Environ(), "HTTPS_PROXY=http://127.0.0.1:1082")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		log.Printf("AI assist error: %v, stderr: %s", err, stderr.String())
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "AI call failed: " + err.Error()})
		return
	}

	result := sanitizeSQL(strings.TrimSpace(stdout.String()))
	if result == "" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "AI did not return valid SQL"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"sql": result})
}

// sanitizeSQL extracts a valid CREATE TABLE statement from AI output.
func sanitizeSQL(raw string) string {
	// Strip markdown fences.
	raw = strings.ReplaceAll(raw, "```sql", "")
	raw = strings.ReplaceAll(raw, "```", "")
	raw = strings.TrimSpace(raw)

	// Try to extract CREATE TABLE ... ;
	if m := createTableRe.FindString(raw); m != "" {
		return strings.TrimSpace(m)
	}

	// If it starts with CREATE TABLE, return as-is (might be missing semicolon).
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "CREATE TABLE") {
		return raw
	}

	return ""
}
