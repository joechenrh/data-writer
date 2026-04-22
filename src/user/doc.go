// Package user holds user-provided per-column generators. The tree ships with
// an empty stub so the main binary builds cleanly when no user code is set;
// EC2 launcher drops user_gens.go here and runs cmd/codegen to produce
// registry_gen.go with gen.Register() calls.
package user
