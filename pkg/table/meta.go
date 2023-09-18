package table

import "go-dbms/pkg/types"

// metadata represents the metadata for the table stored in a json file.
type metadata struct {
	Indexes      []*metaIndex              `json:"indexes"`
	PrimaryKey   *string                   `json:"primary_key"`
	ColumnsOrder []string                  `json:"columns_order"`
	Columns      map[string]types.TypeCode `json:"columns"`
}

type metaIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Uniq    bool     `json:"uniq"`
}
