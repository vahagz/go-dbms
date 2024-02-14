package function

import (
	"go-dbms/pkg/types"
)

const MUL FunctionType = "MUL"

func init() {
	functions[MUL] = func(row map[string]types.DataType, args []types.DataType) types.DataType {
		var val intType
	
		for _, arg := range args {
			v, err := arg.Cast(intMeta)
			if err != nil {
				panic(err)
			}
	
			val *= v.Value().(intType)
		}
	
		return types.Type(intMeta).Set(val)
	}
}
