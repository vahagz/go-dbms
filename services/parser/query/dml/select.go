package dml

import (
	"go-dbms/services/parser/query"
)

type QuerySelect struct {
	query.Query
	Columns    []string    `json:"columns"`
	DB         string      `json:"db"`
	Table      string      `json:"table"`
	Where      *where      `json:"where"`
	WhereIndex *whereIndex `json:"where_index"`
}
