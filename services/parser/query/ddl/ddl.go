package ddl

import (
	"errors"
	"fmt"
	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/ddl/create"
	"text/scanner"
)

func Parse(s *scanner.Scanner, queryType query.QueryType) (query.Querier, error) {
	switch queryType {
		case query.CREATE: return create.Parse(s)
		// case query.DROP:   return drop.Parse(data)
		default:           return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", queryType))
	}
}
