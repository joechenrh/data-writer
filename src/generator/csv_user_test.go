package generator

import (
	"math/rand"
	"strings"
	"testing"

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
