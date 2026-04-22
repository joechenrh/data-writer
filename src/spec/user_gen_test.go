package spec

import (
	"math/rand"
	"testing"

	"dataWriter/src/gen"
)

func TestGenerateRespectsUserFunc(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	gen.Register("uid", func(c *gen.Ctx) any { return int64(123) })

	c := &ColumnSpec{OrigName: "uid", SQLType: "bigint"}
	buf := gen.NewRowBuffer([]string{"uid"})
	rng := rand.New(rand.NewSource(1))

	v, def := c.generateWithUser(0, rng, buf)
	if def != 1 {
		t.Fatalf("defLevel = %d; want 1", def)
	}
	if v != int64(123) {
		t.Fatalf("value = %#v; want int64(123)", v)
	}
	if got := buf.CurrentIndex(); got != 1 {
		t.Fatalf("buf.CurrentIndex() = %d; want 1 (column committed + advanced)", got)
	}
}

func TestGenerateFallsBackWhenNoUserFunc(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	c := &ColumnSpec{OrigName: "other", SQLType: "bigint"}
	buf := gen.NewRowBuffer([]string{"other"})
	rng := rand.New(rand.NewSource(1))

	v, _ := c.generateWithUser(0, rng, buf)
	if _, ok := v.(int); !ok { // builtin generateInt returns int
		t.Fatalf("expected fallback to builtin generateInt (int), got %T", v)
	}
	if got := buf.CurrentIndex(); got != 1 {
		t.Fatalf("buf.CurrentIndex() = %d; want 1 (column committed + advanced)", got)
	}
}

// TestUserFuncReadsSiblingColumn exercises the integration: a second-column
// user generator reads the first-column value via ctx.Int64(...), proving
// that generateWithUser both writes to the RowBuffer and makes committed
// values visible to subsequent columns.
func TestUserFuncReadsSiblingColumn(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	gen.Register("uid", func(c *gen.Ctx) any { return int64(42) })
	gen.Register("uid_doubled", func(c *gen.Ctx) any {
		return c.Int64("uid") * 2
	})

	specs := []*ColumnSpec{
		{OrigName: "uid", SQLType: "bigint"},
		{OrigName: "uid_doubled", SQLType: "bigint"},
	}
	buf := gen.NewRowBuffer([]string{"uid", "uid_doubled"})
	rng := rand.New(rand.NewSource(1))

	v1, _ := specs[0].generateWithUser(0, rng, buf)
	if v1 != int64(42) {
		t.Fatalf("col 0 value = %#v; want int64(42)", v1)
	}

	v2, _ := specs[1].generateWithUser(0, rng, buf)
	if v2 != int64(84) {
		t.Fatalf("col 1 value = %#v; want int64(84) (= uid * 2)", v2)
	}
}

// TestUserFuncNullReturn covers the nil return path: user func returns nil,
// generateWithUser yields ("\\N", 0), and the column becomes IsNull for siblings.
func TestUserFuncNullReturn(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	gen.Register("maybe", func(c *gen.Ctx) any { return nil })

	c := &ColumnSpec{OrigName: "maybe", SQLType: "bigint"}
	buf := gen.NewRowBuffer([]string{"maybe"})
	rng := rand.New(rand.NewSource(1))

	v, def := c.generateWithUser(0, rng, buf)
	if def != 0 {
		t.Fatalf("defLevel = %d; want 0 for nil return", def)
	}
	if v != "\\N" {
		t.Fatalf("value = %#v; want \"\\\\N\"", v)
	}
	// Verify siblings see it as NULL.
	ctx := &gen.Ctx{Buf: buf}
	if !ctx.IsNull("maybe") {
		t.Fatalf("ctx.IsNull(\"maybe\") = false; want true")
	}
}
