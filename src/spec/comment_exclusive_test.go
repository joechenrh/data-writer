package spec

import (
	"strings"
	"testing"
)

func TestParseCommentSetExcludesOthers(t *testing.T) {
	cases := []string{
		`set=["a","b"],mean=10`,
		`set=["a","b"],stddev=5`,
		`set=["a","b"],order=total_order`,
		`set=["a","b"],compress=50`,
		`set=["a","b"],max_length=20`,
		`set=["a","b"],min_length=10`,
		`mean=10,set=[1,2,3]`,
	}
	for _, c := range cases {
		spec := &ColumnSpec{OrigName: "c"}
		err := spec.parseComment(c)
		if err == nil {
			t.Fatalf("expected error for %q, got nil", c)
		}
		if !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("expected mutually-exclusive error for %q, got: %v", c, err)
		}
	}
}

func TestParseCommentMeanStddevExcludesOrder(t *testing.T) {
	cases := []string{
		`mean=100,order=total_order`,
		`stddev=15,order=partial_order`,
		`mean=100,stddev=15,order=random_order`,
		`order=total_order,mean=50`,
	}
	for _, c := range cases {
		spec := &ColumnSpec{OrigName: "c"}
		err := spec.parseComment(c)
		if err == nil {
			t.Fatalf("expected error for %q, got nil", c)
		}
		if !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("expected mutually-exclusive error for %q, got: %v", c, err)
		}
	}
}

func TestParseCommentMeanStddevWithoutOrder(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	if err := spec.parseComment("mean=100,stddev=15"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Mean != 100 || spec.StdDev != 15 {
		t.Fatalf("mean/stddev not set: mean=%d stddev=%d", spec.Mean, spec.StdDev)
	}
}

func TestParseCommentOrderWithoutDistribution(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	if err := spec.parseComment("order=partial_order,null_percent=10"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Order != NumericPartialOrder {
		t.Fatalf("order not set, got %v", spec.Order)
	}
}

func TestParseCommentSetWithNullPercent(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	if err := spec.parseComment(`set=["a","b"],null_percent=20`); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.NullPercent != 20 || len(spec.ValueSet) != 2 {
		t.Fatalf("set+null_percent not parsed correctly: %+v", spec)
	}
}

func TestParseCommentUnknownOption(t *testing.T) {
	spec := &ColumnSpec{OrigName: "c"}
	err := spec.parseComment("foo=bar")
	if err == nil {
		t.Fatalf("expected error for unknown option, got nil")
	}
	if !strings.Contains(err.Error(), "unknown comment option") {
		t.Fatalf("expected unknown-option error, got: %v", err)
	}
}
