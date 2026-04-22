package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sampleUser = `package user

import "dataWriter/src/gen"

func UserId(ctx *gen.Ctx) any {
	return int64(42)
}

// Not a GenFunc — should be ignored.
func helper(x int) int { return x + 1 }

// Wrong signature — should be ignored.
func BadFunc() any { return nil }

func DeviceFinger(ctx *gen.Ctx) any {
	return "abcd"
}
`

func TestScanAndEmit(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "user_gens.go"), []byte(sampleUser), 0644); err != nil {
		t.Fatalf("write sample: %v", err)
	}
	out := filepath.Join(dir, "registry_gen.go")

	if err := run(dir, out); err != nil {
		t.Fatalf("run: %v", err)
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read out: %v", err)
	}
	text := string(got)

	wantLines := []string{
		`package user`,
		`"dataWriter/src/gen"`,
		`gen.Register("user_id", UserId)`,
		`gen.Register("device_finger", DeviceFinger)`,
	}
	for _, want := range wantLines {
		if !strings.Contains(text, want) {
			t.Fatalf("output missing %q\nfull:\n%s", want, text)
		}
	}

	if strings.Contains(text, "BadFunc") || strings.Contains(text, "helper") {
		t.Fatalf("output unexpectedly includes non-matching function:\n%s", text)
	}
}

func TestPascalToSnake(t *testing.T) {
	cases := map[string]string{
		"UserId":       "user_id",
		"DeviceFinger": "device_finger",
		"Billtime":     "billtime",
		"ABCThing":     "abcthing",
		"X":            "x",
	}
	for in, want := range cases {
		if got := pascalToSnake(in); got != want {
			t.Fatalf("pascalToSnake(%q) = %q; want %q", in, got, want)
		}
	}
}
