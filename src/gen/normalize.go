// src/gen/normalize.go
package gen

import (
	"fmt"
	"time"
)

// NormalizeUserValue converts a value returned from a user GenFunc into
// the type the generator hot path expects for the given SQL type.
//
// On nil input, returns (nil, nil) — callers should treat as NULL.
// On type mismatch, returns an error (the worker will turn this into a panic
// with row/column context).
func NormalizeUserValue(v any, sqlType string) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch sqlType {
	case "tinyint", "smallint", "mediumint", "int", "year":
		x, ok := v.(int32)
		if !ok {
			return nil, fmt.Errorf("expected int32 for %s, got %T", sqlType, v)
		}
		return x, nil

	case "bigint":
		x, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 for bigint, got %T", v)
		}
		return x, nil

	case "float", "double":
		x, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64 for %s, got %T", sqlType, v)
		}
		return x, nil

	case "char", "varchar", "blob", "tinyblob", "varbinary", "text":
		x, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for %s, got %T", sqlType, v)
		}
		return x, nil

	case "timestamp", "datetime", "time", "date":
		t, ok := v.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected time.Time for %s, got %T", sqlType, v)
		}
		// Store as time.Time so sibling-read via ctx.Time(col) works. Output
		// writers (CSV format / parquet int64 μs / parquet int32 days) convert
		// to their wire form at the last moment.
		return t, nil

	default:
		return nil, fmt.Errorf("unsupported SQL type for user generator: %q", sqlType)
	}
}
