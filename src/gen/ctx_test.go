package gen

import (
	"math/rand"
	"testing"
	"time"
)

func TestRowBufferSetAndRead(t *testing.T) {
	rb := NewRowBuffer([]string{"a", "b", "c"})
	rb.SetInt64(0, "a", 10)
	rb.SetString(1, "b", "hello")
	rb.SetTime(2, "c", time.Unix(1_700_000_000, 0).UTC())

	// Advance past all three so all columns are readable.
	rb.Advance()
	rb.Advance()
	rb.Advance()

	ctx := &Ctx{RowID: 7, Rng: rand.New(rand.NewSource(1)), Buf: rb}

	if got := ctx.Int64("a"); got != 10 {
		t.Fatalf("ctx.Int64(\"a\") = %d; want 10", got)
	}
	if got := ctx.String("b"); got != "hello" {
		t.Fatalf("ctx.String(\"b\") = %q; want \"hello\"", got)
	}
	if got := ctx.Time("c"); got.Unix() != 1_700_000_000 {
		t.Fatalf("ctx.Time(\"c\").Unix() = %d; want 1700000000", got.Unix())
	}
}

func TestReadUnsetColumnPanics(t *testing.T) {
	rb := NewRowBuffer([]string{"a", "b"})
	rb.SetInt64(0, "a", 1)
	rb.Advance() // a is readable; currentIdx=1. Reading "b" (at idx 1) is not allowed.

	ctx := &Ctx{RowID: 0, Buf: rb}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("reading unset/future column did not panic")
		}
	}()
	_ = ctx.Int64("b")
}

func TestReadUnknownColumnPanics(t *testing.T) {
	rb := NewRowBuffer([]string{"a"})
	ctx := &Ctx{RowID: 0, Buf: rb}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("reading unknown column did not panic")
		}
	}()
	_ = ctx.Int64("zzz")
}

func TestTypeMismatchPanics(t *testing.T) {
	rb := NewRowBuffer([]string{"a"})
	rb.SetString(0, "a", "x")
	rb.Advance()
	ctx := &Ctx{RowID: 0, Buf: rb}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("reading string column as Int64 did not panic")
		}
	}()
	_ = ctx.Int64("a")
}

func TestIsNull(t *testing.T) {
	rb := NewRowBuffer([]string{"a", "b"})
	rb.SetNull(0, "a")
	rb.SetInt64(1, "b", 9)
	rb.Advance()
	rb.Advance()
	ctx := &Ctx{RowID: 0, Buf: rb}
	if !ctx.IsNull("a") {
		t.Fatalf("IsNull(\"a\") = false; want true")
	}
	if ctx.IsNull("b") {
		t.Fatalf("IsNull(\"b\") = true; want false")
	}
}
