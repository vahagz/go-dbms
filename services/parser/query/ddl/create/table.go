package create

import (
	"go-dbms/pkg/column"
)

type QueryCreateTable struct {
	*QueryCreate
	Database string                   `json:"database"`
	Name     string                   `json:"name"`
	Columns  []*column.Column         `json:"columns"`
	Indexes  []*QueryCreateTableIndex `json:"indexes"`
}
