// src/gen/registry_test.go
package gen

import "testing"

func TestRegisterAndLookup(t *testing.T) {
	reset()
	fn := func(c *Ctx) any { return int64(42) }
	Register("user_id", fn)

	got, ok := Lookup("user_id")
	if !ok {
		t.Fatalf("Lookup(\"user_id\") = _, false; want true")
	}
	if got(nil) != int64(42) {
		t.Fatalf("fn(nil) = %v; want 42", got(nil))
	}
}

func TestLookupMissing(t *testing.T) {
	reset()
	if _, ok := Lookup("nope"); ok {
		t.Fatalf("Lookup(\"nope\") = _, true; want false")
	}
}

func TestHasAny(t *testing.T) {
	reset()
	if HasAny() {
		t.Fatalf("HasAny() = true on empty registry; want false")
	}
	Register("x", func(*Ctx) any { return nil })
	if !HasAny() {
		t.Fatalf("HasAny() = false after Register; want true")
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	reset()
	Register("dup", func(*Ctx) any { return nil })
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("second Register for same column did not panic")
		}
	}()
	Register("dup", func(*Ctx) any { return nil })
}
