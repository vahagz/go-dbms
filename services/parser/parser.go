package parser

import (
	"bytes"
	"errors"
	"fmt"
	"text/scanner"

	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/dml"
)

type ParserService interface {
	ParseQuery(in []byte) query.Querier
}

type ParserServiceT struct {  }

func New() *ParserServiceT {
	return &ParserServiceT{}
}

func (ps *ParserServiceT) ParseQuery(data []byte) (query.Querier, error) {
	s := &scanner.Scanner{}
	var qt query.QueryType
	s.Init(bytes.NewReader(data))

	if tok := s.Scan(); tok != scanner.EOF {
		qt = query.QueryType(s.TokenText())
		switch qt {
			// case query.CREATE, query.DROP:
			// 	return ddl.Parse(data, qt)
			case query.DELETE, query.INSERT, query.SELECT, query.UPDATE:
				return dml.Parse(s, qt)
		}
	}

	return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", qt))
}

// func (ps *ParserServiceT) ParseQuery(data []byte) (query.Querier, error) {
// 	q := &query.Query{}
// 	err := json.Unmarshal(data, &q)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "invalid json")
// 	}

// 	switch q.Type {
// 		case query.CREATE, query.DROP:
// 			return ddl.Parse(data, q.Type)
// 		case query.DELETE, query.INSERT, query.SELECT, query.UPDATE:
// 			return dml.Parse(data, q.Type)
// 		default:
// 			return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", q.Type))
// 	}
// }
