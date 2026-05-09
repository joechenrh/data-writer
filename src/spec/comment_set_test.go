package spec

import (
	"reflect"
	"testing"
)

func TestSplitCommentOptsWithSet(t *testing.T) {
	opts, err := splitCommentOpts("null_percent=10,set=[\"a\",\"b\"],max_length=64")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"null_percent=10", "set=[\"a\",\"b\"]", "max_length=64"}
	if !reflect.DeepEqual(opts, want) {
		t.Fatalf("opts mismatch. got=%v want=%v", opts, want)
	}
}

func TestParseCommentSet(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	if err := spec.parseComment("set=[\"active\",\"inactive\"]"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(spec.ValueSet, []string{"active", "inactive"}) {
		t.Fatalf("ValueSet mismatch: %v", spec.ValueSet)
	}
	if len(spec.IntSet) != 0 {
		t.Fatalf("expected IntSet empty, got: %v", spec.IntSet)
	}
}

func TestParseCommentSetInt(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	if err := spec.parseComment("set=[0,1,2]"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(spec.ValueSet) != 0 {
		t.Fatalf("expected ValueSet empty, got: %v", spec.ValueSet)
	}
	if !reflect.DeepEqual(spec.IntSet, []int64{0, 1, 2}) {
		t.Fatalf("IntSet mismatch: %v", spec.IntSet)
	}
}

func TestParseCommentSetInvalid(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	if err := spec.parseComment("set=[active,inactive]"); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestParseCommentSetInvalidType(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	if err := spec.parseComment("set=123"); err == nil {
		t.Fatalf("expected error, got nil")
	}
}
