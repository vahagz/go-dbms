package create

import (
	"errors"
	"fmt"
	"go-dbms/services/parser/query"
	"text/scanner"
)

type QueryCreateTarget string

const (
	DATABASE QueryCreateTarget = "DATABASE"
	TABLE    QueryCreateTarget = "TABLE"
	INDEX    QueryCreateTarget = "INDEX"
)

type Creater interface {
	query.QueryParser
	GetTarget() QueryCreateTarget
}

type QueryCreate struct {
	*query.Query
	Target QueryCreateTarget `json:"target"`
}

func (qc *QueryCreate) GetTarget() QueryCreateTarget {
	return qc.Target
}

func Parse(s *scanner.Scanner) (Creater, error) {
	var q Creater
	
	s.Scan()
	target := QueryCreateTarget(s.TokenText())

	switch target {
		// case DATABASE: q = &QueryCreateDatabase{QueryCreate: &QueryCreate{Query: &query.Query{Type: query.CREATE}}}
		case TABLE:    q = &QueryCreateTable{QueryCreate: &QueryCreate{Query: &query.Query{Type: query.CREATE}}}
		// case INDEX:    q = &QueryCreateIndex{QueryCreate: &QueryCreate{Query: &query.Query{Type: query.CREATE}}}
		default:       return nil, errors.New(fmt.Sprintf("unsupported create target: '%s'", target))
	}

	return q, q.Parse(s)
}
