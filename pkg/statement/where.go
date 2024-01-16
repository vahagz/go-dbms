package statement

import (
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
)

type WhereStatement struct {
	And       []*WhereStatement `json:"and,omitempty"`
	Or        []*WhereStatement `json:"or,omitempty"`
	Statement *Statement        `json:"statement,omitempty"`
}

func (ws *WhereStatement) Compare(row map[string]types.DataType) bool {
	if ws.Statement != nil {
		target := row[ws.Statement.Column()]
		return target.Compare(ws.Statement.Operator(), ws.Statement.Value())
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

func NewStatement(column, operator string, value types.DataType) *Statement {
	return &Statement{column, operator, value}
}

func Where(column, operator string, value types.DataType) *WhereStatement {
	return &WhereStatement{Statement: NewStatement(column, operator, value)}
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
