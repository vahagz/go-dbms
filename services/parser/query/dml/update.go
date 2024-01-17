package dml

import (
	"go-dbms/pkg/statement"
	"go-dbms/services/parser/query"
)

type QueryUpdate struct {
	query.Query
	DB     string                   `json:"db"`
	Table  string                   `json:"table"`
	Values dataMap                  `json:"values"`
	Where  statement.WhereStatement `json:"where"`
}
