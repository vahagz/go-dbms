package eval

import (
	"fmt"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/function"
	"go-dbms/services/parser/query/dml/projection"
)

func Eval(row types.DataRow, p *projection.Projection) types.DataType {
	switch p.Type {
		case projection.LITERAL:
			return p.Literal
		case projection.IDENTIFIER:
			return row[p.Name]
		case projection.FUNCTION:
			argVals := make([]types.DataType, 0, len(p.Arguments))
			for _, arg := range p.Arguments {
				argVals = append(argVals, Eval(row, arg))
			}
			return function.Eval(function.FunctionType(p.Name), row, argVals)
	}

	panic(fmt.Errorf("invalid projection:'%v'", p.Type))
}
