package create

import (
	"go-dbms/pkg/column"
)

type QueryCreateTable struct {
	QueryCreate
	Columns []*column.Column
	Indexes map[string]*QueryCreateTableIndex
}
