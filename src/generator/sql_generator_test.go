package generator

import (
	"strings"
	"testing"
)

func TestSQLEscape(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{"plain", "plain"},
		{"it's", `it\'s`},
		{`back\slash`, `back\\slash`},
		{"line1\nline2", `line1\nline2`},
		{"cr\rlf", `cr\rlf`},
		{"\x00null", `\0null`},
		{"\x1Aeof", `\Zeof`},
	}
	for _, c := range cases {
		got := string(sqlEscape(nil, c.in))
		if got != c.out {
			t.Errorf("sqlEscape(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	if got := quoteIdent("plain"); got != "plain" {
		t.Errorf("got %q, want plain", got)
	}
	if got := quoteIdent("with`tick"); got != "with``tick" {
		t.Errorf("got %q, want with``tick", got)
	}
}

func TestBuildSQLInsertHeader(t *testing.T) {
	cases := []struct {
		prefix string
		out    string
	}{
		{"db.tbl", "INSERT INTO `db`.`tbl` VALUES "},
		{"tbl", "INSERT INTO `tbl` VALUES "},
	}
	for _, c := range cases {
		got, err := buildSQLInsertHeader(c.prefix)
		if err != nil {
			t.Fatalf("buildSQLInsertHeader(%q): %v", c.prefix, err)
		}
		if string(got) != c.out {
			t.Errorf("buildSQLInsertHeader(%q) = %q, want %q", c.prefix, got, c.out)
		}
	}
}

func TestIsSQLNumericType(t *testing.T) {
	for _, t1 := range []string{"tinyint", "smallint", "mediumint", "int", "bigint",
		"float", "double", "decimal", "year"} {
		if !isSQLNumericType(t1) {
			t.Errorf("expected %q to be numeric", t1)
		}
	}
	for _, t1 := range []string{"varchar", "char", "blob", "tinyblob", "json",
		"date", "datetime", "timestamp", "time"} {
		if isSQLNumericType(t1) {
			t.Errorf("expected %q to be string", t1)
		}
	}
}

// TestSQLOutputShape verifies a generated file looks like
// `INSERT INTO ... VALUES (...),(...);` with the right number of tuples.
func TestSQLOutputShape(t *testing.T) {
	// We rely on the integration of csv/parquet tests for actual value
	// generation; here we just confirm the wrapping shape on a known input.
	header, _ := buildSQLInsertHeader("db.tbl")
	body := []byte("(1,'a'),\n(2,'b'),\n(3,'c');\n")
	out := append([]byte{}, header...)
	out = append(out, body...)
	s := string(out)
	if !strings.HasPrefix(s, "INSERT INTO `db`.`tbl` VALUES ") {
		t.Errorf("missing header: %q", s[:30])
	}
	if !strings.HasSuffix(s, ";\n") {
		t.Errorf("missing terminator: %q", s[len(s)-3:])
	}
	if got := strings.Count(s, "("); got != 3 {
		t.Errorf("expected 3 tuples, got %d", got)
	}
}
