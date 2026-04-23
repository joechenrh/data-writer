package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

// workspaceMu serializes operations on serverWorkspace/src/user/ so concurrent
// validate requests don't interleave.
var workspaceMu sync.Mutex

type validateGeneratorsRequest struct {
	GeneratorsGo string `json:"generators_go"`
}

// handleValidateGenerators writes generators_go into src/user/, regenerates
// registry_gen.go, and runs `go build -o /dev/null ./src` in the workspace.
// Returns 200 on success; 400 with stderr on any failure (parse, codegen, build).
//
// Requires the server to be started with -workspace pointing at a data-writer
// source tree.
func handleValidateGenerators(w http.ResponseWriter, r *http.Request) {
	var req validateGeneratorsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.GeneratorsGo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "generators_go is required"})
		return
	}
	if serverWorkspace == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "server was not started with -workspace; validation unavailable"})
		return
	}

	// Fast-path parse check before touching the filesystem.
	if err := validateGeneratorsGo(req.GeneratorsGo); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "parse error: " + err.Error()})
		return
	}

	workspaceMu.Lock()
	defer workspaceMu.Unlock()

	userGensPath := filepath.Join(serverWorkspace, "src", "user", "user_gens.go")
	if err := os.WriteFile(userGensPath, []byte(req.GeneratorsGo), 0644); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write user_gens.go: " + err.Error()})
		return
	}

	if out, err := runIn(serverWorkspace, "go", "run", "./cmd/codegen", "-in", "./src/user", "-out", "./src/user/registry_gen.go"); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "codegen failed:\n" + out})
		return
	}

	if out, err := runIn(serverWorkspace, "go", "build", "-o", "/dev/null", "./src"); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "go build failed:\n" + out})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// runIn runs name+args in dir and returns combined stdout+stderr.
func runIn(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}
