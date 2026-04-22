package server

import "testing"

func TestSanitizeGeneratorsGoStripsFences(t *testing.T) {
	in := "```go\npackage user\n\nfunc X(ctx *gen.Ctx) any { return 1 }\n```"
	got := sanitizeGeneratorsGo(in)
	if got == "" || got[:12] != "package user" {
		t.Fatalf("expected output starting with package user, got %q", got)
	}
	if got[len(got)-3:] == "```" {
		t.Fatalf("trailing fence not stripped: %q", got)
	}
}

func TestSanitizeGeneratorsGoRejectsMissingPackage(t *testing.T) {
	in := "some random prose"
	if sanitizeGeneratorsGo(in) != "" {
		t.Fatalf("expected empty result for non-Go input")
	}
}

func TestSanitizeGeneratorsGoKeepsCleanOutput(t *testing.T) {
	in := "package user\n\nimport \"dataWriter/src/gen\"\n\nfunc X(ctx *gen.Ctx) any { return 1 }\n"
	if sanitizeGeneratorsGo(in) != in[:len(in)-1] { // TrimSpace removes trailing newline
		t.Fatalf("expected passthrough modulo trim, got %q", sanitizeGeneratorsGo(in))
	}
}
