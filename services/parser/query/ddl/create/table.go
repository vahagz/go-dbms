package create

import (
	"go-dbms/pkg/column"
)

type QueryCreateTable struct {
	*QueryCreate
	Name    string                            `json:"name"`
	Columns []*column.Column                  `json:"columns"`
	Indexes map[string]*QueryCreateTableIndex `json:"indexes"`
}
