package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sampleUser = `package user

import "dataWriter/src/gen"

// gen:column user_id
func UserId(ctx *gen.Ctx) any {
	return int64(42)
}

// Not a GenFunc — should be ignored.
func helper(x int) int { return x + 1 }

// Wrong signature — should be ignored.
func BadFunc() any { return nil }

// gen:column device_finger
func DeviceFinger(ctx *gen.Ctx) any {
	return "abcd"
}
`

// sampleUserWithWeirdDirective tests that the // gen:column directive overrides
// the name derived from pascalToSnake(funcName).
const sampleUserWithWeirdDirective = `package user

import "dataWriter/src/gen"

// gen:column weird_name_X9
func WeirdNameX9(ctx *gen.Ctx) any {
	return "x"
}
`

// sampleUserNoDirective tests the pascalToSnake fallback when no directive is present.
const sampleUserNoDirective = `package user

import "dataWriter/src/gen"

func NormalColumn(ctx *gen.Ctx) any {
	return int64(1)
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

func TestScanAndEmitDirective(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "user_gens.go"), []byte(sampleUserWithWeirdDirective), 0644); err != nil {
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

	// The directive column name must be used verbatim, not derived from the func name.
	if !strings.Contains(text, `gen.Register("weird_name_X9", WeirdNameX9)`) {
		t.Fatalf("output missing directive-based Register call\nfull:\n%s", text)
	}
}

func TestScanAndEmitFallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "user_gens.go"), []byte(sampleUserNoDirective), 0644); err != nil {
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

	// Without a directive, pascalToSnake(funcName) is used as the column name.
	if !strings.Contains(text, `gen.Register("normal_column", NormalColumn)`) {
		t.Fatalf("output missing fallback Register call\nfull:\n%s", text)
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
