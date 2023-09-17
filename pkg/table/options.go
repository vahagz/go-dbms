package table

import (
	"go-dbms/pkg/types"
)

// Options represents the configuration options for the table.
type Options struct {
	ColumnsOrder []string
	Columns      map[string]types.TypeCode
}
