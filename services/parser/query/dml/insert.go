package dml

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query"
)

type QueryInsert struct {
	query.Query
	DB      string    `json:"db"`
	Table   string    `json:"table"`
	Columns []string  `json:"columns"`
	Values  [][]types.DataType `json:"values"`
}
