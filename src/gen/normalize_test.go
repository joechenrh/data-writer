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

func TestNormalizeTimeToParquetInt64(t *testing.T) {
	in := time.Unix(1_700_000_000, 123_000).UTC() // 123_000 ns = 123 μs
	got, err := NormalizeUserValue(in, "timestamp")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := int64(1_700_000_000)*1_000_000 + 123
	if got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestNormalizeTimeToDateInt32(t *testing.T) {
	in := time.Date(1970, 1, 11, 0, 0, 0, 0, time.UTC)
	got, err := NormalizeUserValue(in, "date")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != int32(10) {
		t.Fatalf("got %v, want int32(10) (days since epoch)", got)
	}
}

func TestNormalizeTypeMismatch(t *testing.T) {
	_, err := NormalizeUserValue("oops", "bigint")
	if err == nil {
		t.Fatalf("expected type mismatch error, got nil")
	}
}
