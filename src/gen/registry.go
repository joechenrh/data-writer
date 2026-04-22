// src/gen/registry.go
package gen

// GenFunc is a user-provided generator for a single column.
type GenFunc func(*Ctx) any

var registry = map[string]GenFunc{}

// Register binds a generator to a column name. Panics on duplicates.
// Intended to be called from init() of src/user/registry_gen.go.
func Register(column string, fn GenFunc) {
	if _, dup := registry[column]; dup {
		panic("gen.Register: duplicate generator for column: " + column)
	}
	registry[column] = fn
}

// Lookup returns the registered generator for column, if any.
func Lookup(column string) (GenFunc, bool) {
	fn, ok := registry[column]
	return fn, ok
}

// HasAny reports whether any column has a user generator registered.
func HasAny() bool { return len(registry) > 0 }

// reset clears the registry. Only for tests.
func reset() { registry = map[string]GenFunc{} }

// ResetForTest clears the registry. Only for tests in other packages.
func ResetForTest() { reset() }
