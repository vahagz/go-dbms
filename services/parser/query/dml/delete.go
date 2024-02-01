package dml

import (
	"text/scanner"

	"go-dbms/services/parser/query"
)

type QueryDelete struct {
	query.Query
	DB         string      `json:"db"`
	Table      string      `json:"table"`
	Where      *where      `json:"where"`
	WhereIndex *whereIndex `json:"where_index"`
}

func (qs *QueryDelete) Parse(s *scanner.Scanner) (err error) {
	return nil
}
