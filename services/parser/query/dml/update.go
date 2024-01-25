package dml

import (
	"go-dbms/services/parser/query"
)

type QueryUpdate struct {
	query.Query
	DB         string      `json:"db"`
	Table      string      `json:"table"`
	Values     dataMap     `json:"values"`
	Where      *where      `json:"where"`
	WhereIndex *whereIndex `json:"where_index"`
}
