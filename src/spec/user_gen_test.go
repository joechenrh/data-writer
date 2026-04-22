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
}

func TestGenerateFallsBackWhenNoUserFunc(t *testing.T) {
	t.Cleanup(gen.ResetForTest)
	c := &ColumnSpec{OrigName: "other", SQLType: "bigint"}
	buf := gen.NewRowBuffer([]string{"other"})
	rng := rand.New(rand.NewSource(1))

	v, _ := c.generateWithUser(0, rng, buf)
	if _, ok := v.(int); !ok { // generateInt returns int
		t.Fatalf("expected fallback to builtin generateInt (int), got %T", v)
	}
}
