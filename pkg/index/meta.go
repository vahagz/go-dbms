package index

import "github.com/vahagz/bptree"

type Meta struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Uniq    bool            `json:"uniq"`
	Options *bptree.Options `json:"options"`
}
