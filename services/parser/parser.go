package parser

import (
	"errors"
	"fmt"
	"text/scanner"

	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/ddl"
	"go-dbms/services/parser/query/dml"
)

type ParserServiceT struct {  }

func New() *ParserServiceT {
	return &ParserServiceT{}
}

func (ps *ParserServiceT) ParseQuery(s *scanner.Scanner) (query.Querier, error) {
	qt := query.QueryType(s.TokenText())
	switch qt {
		case query.CREATE, query.DROP:
			return ddl.Parse(s, qt)
		case query.DELETE, query.INSERT, query.SELECT, query.UPDATE, query.PREPARE:
			return dml.Parse(s, qt, ps)
	}
	return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", qt))
}
