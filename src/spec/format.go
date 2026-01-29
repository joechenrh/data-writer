package spec

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
)

// DisplaySQLType returns a formatted SQL type for preview output.
func (c *ColumnSpec) DisplaySQLType() string {
	switch c.SQLType {
	case "decimal":
		if c.Precision > 0 && c.Scale > 0 {
			return fmt.Sprintf("decimal(%d,%d)", c.Precision, c.Scale)
		}
		if c.Precision > 0 {
			return fmt.Sprintf("decimal(%d)", c.Precision)
		}
	case "char", "varchar", "binary", "varbinary":
		if c.TypeLen > 0 {
			return fmt.Sprintf("%s(%d)", c.SQLType, c.TypeLen)
		}
	}
	return c.SQLType
}

// FormatSpecsTable renders a human-readable table for column specs.
func FormatSpecsTable(specs []*ColumnSpec) string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, "Name\tType\tNull%\tUnique\tOrder\tSet")
	for _, c := range specs {
		nullPercent := "-"
		if c.NullPercent > 0 {
			nullPercent = strconv.Itoa(c.NullPercent)
		}

		unique := "-"
		if c.IsUnique {
			unique = "yes"
		}

		order := "-"
		switch c.Order {
		case NumericTotalOrder:
			order = "total"
		case NumericPartialOrder:
			order = "partial"
		case NumericRandomOrder:
			order = "random"
		}

		set := "-"
		if len(c.ValueSet) > 0 {
			set = strings.Join(c.ValueSet, "|")
		} else if len(c.IntSet) > 0 {
			vals := make([]string, 0, len(c.IntSet))
			for _, v := range c.IntSet {
				vals = append(vals, strconv.FormatInt(v, 10))
			}
			set = strings.Join(vals, "|")
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			c.OrigName,
			c.DisplaySQLType(),
			nullPercent,
			unique,
			order,
			set,
		)
	}

	_ = w.Flush()
	return buf.String()
}
