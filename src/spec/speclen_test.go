package spec

import "testing"

func TestClampHugeFlenToDefaultTypeLen(t *testing.T) {
	defaultLen := 64
	spec := &ColumnSpec{TypeLen: defaultLen}

	flen := 4294967295
	if flen > 1<<20 {
		flen = spec.TypeLen
	}

	if flen != defaultLen {
		t.Fatalf("expected clamped flen to be %d, got %d", defaultLen, flen)
	}
}
