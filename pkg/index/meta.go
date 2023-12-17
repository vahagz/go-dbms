package index

import "go-dbms/pkg/bptree"

type Meta struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Uniq    bool            `json:"uniq"`
	Options *bptree.Options `json:"options"`
}
