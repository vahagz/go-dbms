package dml

import (
	"go-dbms/services/parser/query"
	"text/scanner"
)

type QueryInsert struct {
	query.Query
	DB      string    `json:"db"`
	Table   string    `json:"table"`
	Columns []string  `json:"columns"`
	Values  []dataRow `json:"values"`
}

func (qs *QueryInsert) Parse(s *scanner.Scanner) (err error) {
	return nil
}
