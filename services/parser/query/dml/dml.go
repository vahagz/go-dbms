package dml

import (
	"errors"
	"fmt"
	"text/scanner"

	"go-dbms/pkg/index"
	"go-dbms/services/parser/query"
)

func Parse(s *scanner.Scanner, queryType query.QueryType) (query.Querier, error) {
	var q query.Querier

	switch queryType {
		case query.DELETE:  q = &QueryDelete{}
		case query.INSERT:  q = &QueryInsert{}
		case query.SELECT:  q = &QuerySelect{}
		case query.UPDATE:  q = &QueryUpdate{}
		case query.PREPARE: q = &QueryPrepare{}
		default:            return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", queryType))
	}

	return q, q.Parse(s)
}

type WhereIndex struct {
	Name        string        `json:"name"`
	FilterStart *index.Filter `json:"filter_start"`
	FilterEnd   *index.Filter `json:"filter_end"`
}
