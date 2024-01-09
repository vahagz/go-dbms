package data

import (
	"os"

	"go-dbms/pkg/column"
)

// defaultOptions to be used by New().
var DefaultOptions = Options{
	PageSize: os.Getpagesize(),
}

// Options represents the configuration options for the df.
type Options struct {
	// PageSize to be for file I/O. All reads and writes will always
	// be done with pages of this size. Must be multiple of 4096.
	PageSize int

	// list of columns to distinguish data in records, this is general
	// for all records, and is stored in metadata of table
	Columns []*column.Column
}
