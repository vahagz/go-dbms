package bptree

// Options represents the configuration options for the B+ tree index.
type Options struct {
	// PageSize to be for file I/O. All reads and writes will always
	// be done with pages of this size. Must be multiple of 4096.
	PageSize int `json:"page_size"`

	// MaxKeySize represents the maximum size allowed for the key.
	// Put call with keys larger than this will result in error.
	MaxKeySize int `json:"max_key_size"`

	// Count of columns of key
	KeyCols int `json:"key_cols"`

	// Count of columns of suffix in key
	// If set 0, bptree will take value from counter
	SuffixCols int `json:"suffix_cols"`

	// Max size allowed for suffix
	MaxSuffixSize int `json:"max_suffix_size"`

	// MaxValueSize represents the maximum size allowed for the value.
	// Put call with values larger than this will result in error.
	// Branching factor reduces as this size increases. So smaller
	// the better.
	MaxValueSize int `json:"max_value_size"`

	// number of children per node
	Degree int `json:"degree"`

	// if set True, values inserted must be unique, othervise values can repeat
	// but BPTree will add extra bytes at end of key to maintain uniqueness
	Uniq bool `json:"uniq"`
}

type PutOptions struct {
	Update bool
}

type ScanOptions struct {
	Key [][]byte
	Reverse, Strict bool
}
