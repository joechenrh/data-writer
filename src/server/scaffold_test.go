package server

import (
	"strings"
	"testing"
)

func TestBuildScaffold(t *testing.T) {
	sql := `CREATE TABLE app.users (
		user_id BIGINT,
		device_finger VARCHAR(12),
		created_at TIMESTAMP
	);`
	got, err := buildScaffold(sql)
	if err != nil {
		t.Fatalf("buildScaffold: %v", err)
	}
	for _, want := range []string{
		"package user",
		`import "dataWriter/src/gen"`,
		"// Column: user_id",
		"// func UserId(ctx *gen.Ctx) any {",
		"// Column: device_finger",
		"// func DeviceFinger(ctx *gen.Ctx) any {",
		"// Column: created_at",
		"// func CreatedAt(ctx *gen.Ctx) any {",
		"return int64(0)",
		"return \"\"",
		"return time.Time{}",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("scaffold missing %q\nfull:\n%s", want, got)
		}
	}
}

func TestBuildScaffoldBadSQL(t *testing.T) {
	_, err := buildScaffold("not a create table")
	if err == nil {
		t.Fatalf("expected error for invalid SQL")
	}
}

func TestSnakeToPascal(t *testing.T) {
	cases := map[string]string{
		"user_id":       "UserId",
		"device_finger": "DeviceFinger",
		"billtime":      "Billtime",
		"a_b_c":         "ABC",
	}
	for in, want := range cases {
		if got := snakeToPascal(in); got != want {
			t.Fatalf("snakeToPascal(%q) = %q; want %q", in, got, want)
		}
	}
}
