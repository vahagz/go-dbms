package dml

import (
	"go-dbms/pkg/statement"
	"go-dbms/services/parser/query"
)

type QuerySelect struct {
	query.Query
	Columns []string                 `json:"columns"`
	DB      string                   `json:"db"`
	Table   string                   `json:"table"`
	Where   statement.WhereStatement `json:"where"`
}
