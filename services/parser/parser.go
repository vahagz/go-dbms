package parser

import (
	"encoding/json"
	"fmt"

	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/ddl"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

type ParserService interface {
	ParseQuery(in []byte) query.Querier
}

type ParserServiceT struct {  }

func New() *ParserServiceT {
	return &ParserServiceT{}
}

func (ps *ParserServiceT) ParseQuery(data []byte) (query.Querier, error) {
	q := &query.Query{}
	err := json.Unmarshal(data, &q)
	if err != nil {
		return nil, errors.Wrap(err, "invalid json")
	}

	switch q.Type {
		case query.CREATE, query.DROP:
			return ddl.Parse(data, q.Type)
		case query.DELETE, query.INSERT, query.SELECT, query.UPDATE:
			return dml.Parse(data, q.Type)
		default:
			return nil, errors.New(fmt.Sprintf("unsupported query type: '%s'", q.Type))
	}
}
