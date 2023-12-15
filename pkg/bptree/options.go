package bptree

import "os"

// defaultOptions to be used by New().
var defaultOptions = Options{
	PageSize:     os.Getpagesize(),
	MaxKeySize:   100,
	MaxValueSize: 12,
}

// Options represents the configuration options for the B+ tree index.
type Options struct {
	// PageSize to be for file I/O. All reads and writes will always
	// be done with pages of this size. Must be multiple of 4096.
	PageSize int

	// MaxKeySize represents the maximum size allowed for the key.
	// Put call with keys larger than this will result in error.
	// Branching factor reduces as this size increases. So smaller
	// the better.
	MaxKeySize int

	// Count of columns of key
	KeyCols int

	// MaxValueSize represents the maximum size allowed for the value.
	// Put call with values larger than this will result in error.
	// Branching factor reduces as this size increases. So smaller
	// the better.
	MaxValueSize int

	// number of children per node
	Degree int

	// if set True, values inserted must be unique, othervise values can repeat
	// but BPTree will add extra bytes at end of key to maintain uniqueness
	Uniq bool
}

type PutOptions struct {
	Update bool
}

type ScanOptions struct {
	Key [][]byte
	Reverse, Strict bool
}
