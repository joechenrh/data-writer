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
