package dml

import (
	"go-dbms/pkg/statement"
	"go-dbms/services/parser/query"
)

type QueryDelete struct {
	query.Query
	DB    string                   `json:"db"`
	Table string                   `json:"table"`
	Where statement.WhereStatement `json:"where"`
}
