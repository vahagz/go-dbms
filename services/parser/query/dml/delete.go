package dml

import (
	"go-dbms/services/parser/query"
)

type QueryDelete struct {
	query.Query
	DB         string      `json:"db"`
	Table      string      `json:"table"`
	Where      *where      `json:"where"`
	WhereIndex *whereIndex `json:"where_index"`
}
