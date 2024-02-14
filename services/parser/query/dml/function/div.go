package function

import (
	"go-dbms/pkg/types"
)

const DIV FunctionType = "DIV"

func init() {
	functions[DIV] = func(row map[string]types.DataType, args []types.DataType) types.DataType {
		v1, err := args[0].Cast(intMeta)
		if err != nil {
			panic(err)
		}
	
		v2, err := args[1].Cast(intMeta)
		if err != nil {
			panic(err)
		}
	
		return types.Type(intMeta).Set(v1.Value().(intType) / v2.Value().(intType))
	}
}
