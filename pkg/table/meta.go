package table

import (
	"go-dbms/pkg/column"
	"go-dbms/pkg/index"
)

// metadata represents the metadata for the table stored in a json file.
type metadata struct {
	Indexes    []*index.Meta             `json:"indexes"`
	PrimaryKey *string                   `json:"primary_key"`
	Columns    []*column.Column          `json:"columns"`
	ColumnsMap map[string]*column.Column `json:"-"`
}
