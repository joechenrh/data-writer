// src/gen/normalize_test.go
package gen

import (
	"testing"
	"time"
)

func TestNormalizeMatchingTypes(t *testing.T) {
	cases := []struct {
		sqlType string
		in      any
		want    any
	}{
		{"int", int32(7), int32(7)},
		{"bigint", int64(9_000_000_000), int64(9_000_000_000)},
		{"double", float64(3.14), float64(3.14)},
		{"varchar", "hello", "hello"},
	}
	for _, tc := range cases {
		got, err := NormalizeUserValue(tc.in, tc.sqlType)
		if err != nil {
			t.Fatalf("%s: unexpected err: %v", tc.sqlType, err)
		}
		if got != tc.want {
			t.Fatalf("%s: got %#v, want %#v", tc.sqlType, got, tc.want)
		}
	}
}

func TestNormalizeNil(t *testing.T) {
	got, err := NormalizeUserValue(nil, "bigint")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != nil {
		t.Fatalf("nil returned non-nil: %#v", got)
	}
}

// Time-typed columns now round-trip as time.Time so sibling-read via ctx.Time
// works. Output writers (csv format / parquet int64 μs / parquet int32 days)
// convert to wire form at the boundary.
func TestNormalizeTimeRoundTrip(t *testing.T) {
	in := time.Unix(1_700_000_000, 123_000).UTC()
	for _, sqlType := range []string{"timestamp", "datetime", "time"} {
		got, err := NormalizeUserValue(in, sqlType)
		if err != nil {
			t.Fatalf("%s: unexpected err: %v", sqlType, err)
		}
		gotT, ok := got.(time.Time)
		if !ok {
			t.Fatalf("%s: got %T, want time.Time", sqlType, got)
		}
		if !gotT.Equal(in) {
			t.Fatalf("%s: got %v, want %v", sqlType, gotT, in)
		}
	}
}

func TestNormalizeDateRoundTrip(t *testing.T) {
	in := time.Date(1970, 1, 11, 0, 0, 0, 0, time.UTC)
	got, err := NormalizeUserValue(in, "date")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	gotT, ok := got.(time.Time)
	if !ok {
		t.Fatalf("got %T, want time.Time", got)
	}
	if !gotT.Equal(in) {
		t.Fatalf("got %v, want %v", gotT, in)
	}
}

func TestNormalizeTypeMismatch(t *testing.T) {
	_, err := NormalizeUserValue("oops", "bigint")
	if err == nil {
		t.Fatalf("expected type mismatch error, got nil")
	}
}
