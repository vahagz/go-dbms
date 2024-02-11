package function

import (
	"errors"

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

const (
	ADD FunctionType = "ADD"
	SUB FunctionType = "SUB"
	MUL FunctionType = "MUL"
	DIV FunctionType = "DIV"
	RES FunctionType = "RES"
)

var functions = map[FunctionType]struct{}{
	ADD: {},
	SUB: {},
	MUL: {},
	DIV: {},
	RES: {},
}

type FunctionBase struct {
	Arguments []string
}

func (ab *FunctionBase) Apply(value ...types.DataType) types.DataType {
	panic(errors.New("unimplemented"))
}

type Function interface {
	Apply(value ...types.DataType) types.DataType
}

func IsFunction(fn string) bool {
	_, ok := functions[FunctionType(fn)]
	return ok
}

func New(name FunctionType, args []string) Function {
	ab := &FunctionBase{args}
	switch name {
		case ADD: return &FunctionADD{FunctionBase: ab}
		case SUB: return &FunctionSUB{FunctionBase: ab}
		case MUL: return &FunctionMUL{FunctionBase: ab}
		case DIV: return &FunctionDIV{FunctionBase: ab}
		case RES: return &FunctionRES{FunctionBase: ab}
		default: panic(errors.New("unknown aggregate function"))
	}
}
