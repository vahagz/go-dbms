package index

import "github.com/vahagz/bptree"

type ScanOptions struct {
	bptree.ScanOptions
}

type IndexOptions struct {
	Columns       []string `json:"columns"`
	Primary       bool     `json:"primary"`
	Uniq          bool     `json:"uniq"`
	AutoIncrement bool     `json:"auto_increment"`
}
