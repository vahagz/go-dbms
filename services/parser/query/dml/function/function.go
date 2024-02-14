package function

import (
	"go-dbms/pkg/types"

	"golang.org/x/exp/constraints"
)

type numeric interface {
	constraints.Float | constraints.Integer
}

type comparable interface {
	constraints.Ordered
}

type FunctionType string

type Function func(row map[string]types.DataType, args []types.DataType) types.DataType

var functions = map[FunctionType]Function{}

func IsFunction(fn string) bool {
	_, ok := functions[FunctionType(fn)]
	return ok
}

func Eval(name FunctionType, row map[string]types.DataType, args []types.DataType) types.DataType {
	return functions[name](row, args)
}
