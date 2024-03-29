package create

import (
	"go-dbms/pkg/index"
	"go-dbms/services/parser/query"
	"text/scanner"
)

type QueryCreateTableIndex struct {
	*index.IndexOptions
	Name string `json:"name"`
}

type QueryCreateIndex struct {
	*QueryCreate
	QueryCreateTableIndex
	Table string `json:"table"`
}

func (qs *QueryCreateIndex) Parse(s *scanner.Scanner, ps query.Parser) (err error) {
	return nil
}
