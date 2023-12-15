package table

import (
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/column"
)

// metadata represents the metadata for the table stored in a json file.
type metadata struct {
	Indexes    []*metaIndex              `json:"indexes"`
	PrimaryKey *string                   `json:"primary_key"`
	Columns    []*column.Column          `json:"columns"`
	ColumnsMap map[string]*column.Column `json:"-"`
}

type metaIndex struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Uniq    bool            `json:"uniq"`
	Options *bptree.Options `json:"options"`
}
