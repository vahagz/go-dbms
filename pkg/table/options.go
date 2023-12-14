package table

import (
	"go-dbms/pkg/column"
)

// Options represents the configuration options for the table.
type Options struct {
	Columns []*column.Column
}

type IndexOptions struct {
	Primary, Uniq bool
}
