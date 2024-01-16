package create

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-dbms/services/parser/query"
)

type QueryCreateTarget string

const (
	DATABASE QueryCreateTarget = "DATABASE"
	TABLE    QueryCreateTarget = "TABLE"
	INDEX    QueryCreateTarget = "INDEX"
)

type Creater interface {
	query.Querier
	GetTarget() QueryCreateTarget
}

type QueryCreate struct {
	*query.Query
	Target QueryCreateTarget `json:"target"`
}

func (qc *QueryCreate) GetTarget() QueryCreateTarget {
	return qc.Target
}

func Parse(data []byte) (Creater, error) {
	var q Creater
	createQuery := &QueryCreate{}
	if err := json.Unmarshal(data, createQuery); err != nil {
		return nil, err
	}

	switch createQuery.Target {
		case DATABASE: q = &QueryCreateDatabase{}
		case TABLE:    q = &QueryCreateTable{}
		case INDEX:    q = &QueryCreateIndex{}
		default:       return nil, errors.New(fmt.Sprintf("unsupported create target: '%s'", createQuery.Target))
	}

	return q, json.Unmarshal(data, &q)
}
