package function

import (
	"go-dbms/pkg/types"
)

const RES FunctionType = "RES"

func init() {
	functions[RES] = func(row map[string]types.DataType, args []types.DataType) types.DataType {
		v1, err := args[0].Cast(intMeta)
		if err != nil {
			panic(err)
		}
	
		v2, err := args[1].Cast(intMeta)
		if err != nil {
			panic(err)
		}
	
		return types.Type(intMeta).Set(v1.Value().(intType) % v2.Value().(intType))
	}
}
