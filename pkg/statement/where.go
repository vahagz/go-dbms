package statement

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"

	"github.com/pkg/errors"
)

type WhereStatement struct {
	And       []*WhereStatement `json:"and,omitempty"`
	Or        []*WhereStatement `json:"or,omitempty"`
	Statement *Statement        `json:"statement,omitempty"`
}

func (ws *WhereStatement) Compare(row types.DataRow) bool {
	if ws.Statement != nil {
		l := eval.Eval(row, ws.Statement.Left)
		r := eval.Eval(row, ws.Statement.Right)
		return l.CompareOp(ws.Statement.Op, r)
	}

	if len(ws.And) != 0 {
		for _, ws := range ws.And {
			if !ws.Compare(row) {
				return false
			}
		}
		return true
	}

	if len(ws.Or) != 0 {
		for _, ws := range ws.Or {
			if ws.Compare(row) {
				return true
			}
		}
		return false
	}

	panic(errors.New("invalid where statement"))
}

func WhereS(s *Statement) *WhereStatement {
	return &WhereStatement{Statement: s}
}

func And(list ...*Statement) *WhereStatement {
	andList := make([]*WhereStatement, 0, len(list))
	for _, s := range list {
		andList = append(andList, WhereS(s))
	}

	return &WhereStatement{
		And: andList,
	}
}

func Or(list ...*Statement) *WhereStatement {
	orList := make([]*WhereStatement, 0, len(list))
	for _, s := range list {
		orList = append(orList, WhereS(s))
	}

	return &WhereStatement{
		Or: orList,
	}
}
