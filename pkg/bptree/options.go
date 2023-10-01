package bptree

import (
	"go-dbms/pkg/freelist"
	"os"
)

// defaultOptions to be used by New().
var defaultOptions = Options{
	ReadOnly:     false,
	FileMode:     0644,
	PageSize:     os.Getpagesize(),
	MaxKeySize:   100,
	MaxValueSize: 12,
}

// Options represents the configuration options for the B+ tree index.
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

	// MaxKeySize represents the maximum size allowed for the key.
	// Put call with keys larger than this will result in error.
	// Branching factor reduces as this size increases. So smaller
	// the better.
	MaxKeySize int

	// MaxValueSize represents the maximum size allowed for the value.
	// Put call with values larger than this will result in error.
	// Branching factor reduces as this size increases. So smaller
	// the better.
	MaxValueSize int

	// PreAlloc can be set to enable pre-allocating pages when the
	// index is initialized. This helps avoid mmap/unmap and truncate
	// overheads during insertions.
	PreAlloc int

	FreelistOptions *freelist.Options
}

type PutOptions struct {
	Uniq   bool
	Update bool
}
