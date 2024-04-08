package dml

import (
	"errors"
	"fmt"
	"text/scanner"

	"go-dbms/pkg/index"
	"go-dbms/services/parser/query"
)

func Parse(s *scanner.Scanner, queryType query.QueryType, ps query.Parser) (query.Querier, error) {
	var q query.QueryParser

	switch queryType {
		case query.DELETE:  q = &QueryDelete{}
		case query.INSERT:  q = &QueryInsert{}
		case query.SELECT:  q = &QuerySelect{}
		case query.UPDATE:  q = &QueryUpdate{}
		case query.PREPARE: q = &QueryPrepare{}
		default:            return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", queryType))
	}

	return q, q.Parse(s, ps)
}

type WhereIndex struct {
	FilterStart *index.Filter
	FilterEnd   *index.Filter
}
