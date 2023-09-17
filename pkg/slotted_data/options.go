package data

import (
	"go-dbms/pkg/types"
	"os"
)

// defaultOptions to be used by New().
var DefaultOptions = Options{
	ReadOnly: false,
	FileMode: 0644,
	PageSize: os.Getpagesize(),
	PreAlloc: 10,
}

// Options represents the configuration options for the df.
type Options struct {
	// ReadOnly mode for index. All mutating operations will return
	// error in this mode.
	ReadOnly bool

	// FileMode for creating the file. Applicable only if when a new
	// index file is being initialized.
	FileMode os.FileMode

	// PageSize to be for file I/O. All reads and writes will always
	// be done with pages of this size. Must be multiple of 4096.
	PageSize int

	// PreAlloc can be set to enable pre-allocating pages when the
	// index is initialized. This helps avoid mmap/unmap and truncate
	// overheads during insertions.
	PreAlloc int

	// list of columns to distinguish data in records, this general for
	// all records, and is stored in metadata page of df
	Columns map[string]types.TypeCode
}
