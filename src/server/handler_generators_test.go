package server

import "testing"

func TestValidateGeneratorsGoOK(t *testing.T) {
	good := `package user

import "dataWriter/src/gen"

func UserId(ctx *gen.Ctx) any { return int64(1) }
`
	if err := validateGeneratorsGo(good); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidateGeneratorsGoSyntaxErr(t *testing.T) {
	bad := `package user

func Broken(ctx *gen.Ctx) any {
	return
` // unclosed brace

	if err := validateGeneratorsGo(bad); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestRowsPerRGCheck(t *testing.T) {
	if err := checkRowsPerRowGroup(10_000_000, 1); err == nil {
		t.Fatalf("10M rows / 1 RG should fail")
	}
	if err := checkRowsPerRowGroup(10_000_000, 10); err != nil {
		t.Fatalf("10M rows / 10 RG should pass, got %v", err)
	}
}
