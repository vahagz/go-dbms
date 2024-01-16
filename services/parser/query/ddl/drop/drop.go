package drop

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-dbms/services/parser/query"
)

type QueryDropTarget string

const (
	DATABASE QueryDropTarget = "DATABASE"
	TABLE    QueryDropTarget = "TABLE"
	INDEX    QueryDropTarget = "INDEX"
)

type Dropper interface {
	query.Querier
	GetTarget() QueryDropTarget
}

type QueryDrop struct {
	query.Query
	Target QueryDropTarget `json:"target"`
}

func (qd *QueryDrop) GetTarget() QueryDropTarget {
	return qd.Target
}

func Parse(data []byte) (Dropper, error) {
	var q Dropper
	dropQuery := &QueryDrop{}
	if err := json.Unmarshal(data, dropQuery); err != nil {
		return nil, err
	}

	switch dropQuery.Target {
		case DATABASE: q = &QueryDropDatabase{}
		case TABLE:    q = &QueryDropTable{}
		case INDEX:    q = &QueryDropIndex{}
		default:       return nil, errors.New(fmt.Sprintf("unsupported drop target: '%s'", dropQuery.Target))
	}

	return q, json.Unmarshal(data, &q)
}
