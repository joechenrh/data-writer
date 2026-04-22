package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadBoundedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "err.txt")

	content := strings.Repeat("abc", 100) // 300 bytes
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := readBoundedFile(path, 64*1024)
	if err != nil {
		t.Fatalf("readBoundedFile: %v", err)
	}
	if string(got) != content {
		t.Fatalf("got %q; want %q", got, content)
	}
}

func TestReadBoundedFileTruncation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.txt")

	content := strings.Repeat("x", 1000)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := readBoundedFile(path, 100)
	if err != nil {
		t.Fatalf("readBoundedFile: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("len(got)=%d; want 100 (truncation)", len(got))
	}
}

func TestReadBoundedFileMissing(t *testing.T) {
	_, err := readBoundedFile("/nonexistent/nope", 1024)
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
}
