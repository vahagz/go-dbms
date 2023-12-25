package statement

import (
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
)

type WhereStatement struct {
	and       []*WhereStatement
	or        []*WhereStatement
	statement *Statement
}

func (ws *WhereStatement) Compare(row map[string]types.DataType) bool {
	if ws.statement != nil {
		target := row[ws.statement.column]
		return target.Compare(ws.statement.operator, ws.statement.value)
	}

	if len(ws.and) != 0 {
		for _, ws := range ws.and {
			if !ws.Compare(row) {
				return false
			}
		}
		return true
	}

	if len(ws.or) != 0 {
		for _, ws := range ws.or {
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
	return &WhereStatement{statement: NewStatement(column, operator, value)}
}

func WhereS(s *Statement) *WhereStatement {
	return &WhereStatement{statement: s}
}

func And(list ...*Statement) *WhereStatement {
	andList := make([]*WhereStatement, 0, len(list))
	for _, s := range list {
		andList = append(andList, WhereS(s))
	}

	return &WhereStatement{
		and: andList,
	}
}

func Or(list ...*Statement) *WhereStatement {
	orList := make([]*WhereStatement, 0, len(list))
	for _, s := range list {
		orList = append(orList, WhereS(s))
	}

	return &WhereStatement{
		or: orList,
	}
}
