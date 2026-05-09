package generator

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"dataWriter/src/gen"
	"dataWriter/src/spec"
)

func TestCSVRespectsUserGenerator(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	gen.Register("uid", func(c *gen.Ctx) any { return int64(42) })
	gen.Register("name", func(c *gen.Ctx) any {
		return "u" + itoa(c.Int64("uid"))
	})

	specs := []*spec.ColumnSpec{
		{OrigName: "uid", SQLType: "bigint"},
		{OrigName: "name", SQLType: "varchar"},
	}

	rng := rand.New(rand.NewSource(1))
	buf := make([]byte, 0, 256)
	buf = generateCSVRow(specs, 0, false, rng, buf, []byte(","), []byte("\n"))

	got := string(buf)
	if !strings.HasPrefix(got, "42,u42\n") {
		t.Fatalf("csv row = %q; want prefix %q", got, "42,u42\n")
	}
}

func itoa(x int64) string {
	const digits = "0123456789"
	if x == 0 {
		return "0"
	}
	var b []byte
	for x > 0 {
		b = append([]byte{digits[x%10]}, b...)
		x /= 10
	}
	return string(b)
}

// TestCSVUserTimeGeneratorFormats verifies that when a user generator for a
// TIMESTAMP column returns a time.Time, the CSV output is a formatted string
// (not the raw microsecond int64).  Regression for bug 3.
func TestCSVUserTimeGeneratorFormats(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	target := time.Date(2026, 4, 21, 12, 34, 56, 0, time.Local)
	gen.Register("ts", func(c *gen.Ctx) any { return target })

	specs := []*spec.ColumnSpec{
		{OrigName: "ts", SQLType: "timestamp"},
	}

	rng := rand.New(rand.NewSource(1))
	buf := make([]byte, 0, 128)
	buf = generateCSVRow(specs, 0, false, rng, buf, []byte(","), []byte("\n"))
	got := string(buf)
	want := target.Format(time.DateTime) + "\n"
	if got != want {
		t.Fatalf("csv row = %q; want %q", got, want)
	}
}

// TestCSVUserTimeSiblingRead verifies that a later user generator can read an
// earlier TIMESTAMP/DATETIME/DATE column via ctx.Time(col). Regression for the
// "stored as int64, ctx.Time expects time.Time" panic.
func TestCSVUserTimeSiblingRead(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	target := time.Date(2026, 4, 21, 12, 34, 0, 0, time.UTC)
	gen.Register("ts", func(c *gen.Ctx) any { return target })
	gen.Register("tag", func(c *gen.Ctx) any {
		return "at_" + c.Time("ts").UTC().Format("15:04")
	})

	specs := []*spec.ColumnSpec{
		{OrigName: "ts", SQLType: "datetime"},
		{OrigName: "tag", SQLType: "varchar"},
	}

	rng := rand.New(rand.NewSource(1))
	buf := make([]byte, 0, 128)
	buf = generateCSVRow(specs, 0, false, rng, buf, []byte(","), []byte("\n"))
	got := string(buf)
	if !strings.Contains(got, "at_12:34") {
		t.Fatalf("csv row = %q; want it to contain at_12:34", got)
	}
}
