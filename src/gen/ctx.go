package gen

import (
	"fmt"
	"math/rand"
	"time"
)

// Ctx is passed to every user GenFunc. RowID is the absolute row number
// across the whole task; Rng is a worker-local random source.
type Ctx struct {
	RowID int64
	Rng   *rand.Rand
	Buf   *RowBuffer
}

// RowBuffer holds the typed values of sibling columns in the current row.
// Only columns with index < currentIdx have been generated and are readable.
type RowBuffer struct {
	names      []string
	indexOf    map[string]int
	currentIdx int
	values     []any // per-column: int32, int64, float64, string, time.Time, or nilMarker
}

type nilMarker struct{}

var nilSentinel = nilMarker{}

// NewRowBuffer creates a buffer for a task with the given column order.
func NewRowBuffer(columnOrder []string) *RowBuffer {
	idx := make(map[string]int, len(columnOrder))
	for i, n := range columnOrder {
		idx[n] = i
	}
	return &RowBuffer{
		names:      columnOrder,
		indexOf:    idx,
		values:     make([]any, len(columnOrder)),
		currentIdx: 0,
	}
}

// Advance marks the next column as done. Call after SetX for column i to
// make column i readable via Ctx accessors for the remaining columns.
func (b *RowBuffer) Advance() { b.currentIdx++ }

// Reset clears committed values and rewinds to column 0. Called between rows.
func (b *RowBuffer) Reset() {
	for i := range b.values {
		b.values[i] = nil
	}
	b.currentIdx = 0
}

// CurrentIndex returns the index of the next column to be generated.
func (b *RowBuffer) CurrentIndex() int { return b.currentIdx }

func (b *RowBuffer) SetInt32(i int, name string, v int32)     { b.values[i] = v }
func (b *RowBuffer) SetInt64(i int, name string, v int64)     { b.values[i] = v }
func (b *RowBuffer) SetFloat64(i int, name string, v float64) { b.values[i] = v }
func (b *RowBuffer) SetString(i int, name string, v string)   { b.values[i] = v }
func (b *RowBuffer) SetTime(i int, name string, v time.Time)  { b.values[i] = v }
func (b *RowBuffer) SetNull(i int, name string)               { b.values[i] = nilSentinel }

func (b *RowBuffer) lookup(col string) any {
	idx, ok := b.indexOf[col]
	if !ok {
		panic(fmt.Sprintf("gen: unknown column %q", col))
	}
	if idx >= b.currentIdx {
		panic(fmt.Sprintf("gen: column %q has not been generated yet "+
			"(current=%d, requested=%d) — only earlier columns in CREATE TABLE are readable",
			col, b.currentIdx, idx))
	}
	return b.values[idx]
}

func (c *Ctx) Int32(col string) int32 {
	v := c.Buf.lookup(col)
	x, ok := v.(int32)
	if !ok {
		panic(fmt.Sprintf("gen: column %q is not int32 (is %T)", col, v))
	}
	return x
}

func (c *Ctx) Int64(col string) int64 {
	v := c.Buf.lookup(col)
	x, ok := v.(int64)
	if !ok {
		panic(fmt.Sprintf("gen: column %q is not int64 (is %T)", col, v))
	}
	return x
}

func (c *Ctx) Float64(col string) float64 {
	v := c.Buf.lookup(col)
	x, ok := v.(float64)
	if !ok {
		panic(fmt.Sprintf("gen: column %q is not float64 (is %T)", col, v))
	}
	return x
}

func (c *Ctx) String(col string) string {
	v := c.Buf.lookup(col)
	x, ok := v.(string)
	if !ok {
		panic(fmt.Sprintf("gen: column %q is not string (is %T)", col, v))
	}
	return x
}

func (c *Ctx) Time(col string) time.Time {
	v := c.Buf.lookup(col)
	x, ok := v.(time.Time)
	if !ok {
		panic(fmt.Sprintf("gen: column %q is not time.Time (is %T)", col, v))
	}
	return x
}

// IsNull reports whether the named sibling column was generated as NULL.
func (c *Ctx) IsNull(col string) bool {
	v := c.Buf.lookup(col)
	_, isNull := v.(nilMarker)
	return isNull
}
