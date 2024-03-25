package function

import (
	"go-dbms/pkg/types"
)

const ADD FunctionType = "ADD"

func init() {
	functions[ADD] = func(row types.DataRow, args []types.DataType) types.DataType {
		var valInt intType
		var valFloat floatType
		isInt := true

		for _, arg := range args {
			if isInt && arg.GetCode() == types.TYPE_FLOAT {
				valFloat = float64(valInt)
				isInt = false
			}

			if isInt {
				v, err := arg.Cast(intMeta)
				if err != nil {
					panic(err)
				}
	
				valInt += v.Value().(intType)
			} else {
				v, err := arg.Cast(floatMeta)
				if err != nil {
					panic(err)
				}
	
				valFloat += v.Value().(floatType)
			}
		}
	
		if isInt {
			return types.Type(intMeta).Set(valInt)
		} else {
			return types.Type(floatMeta).Set(valFloat)
		}
	}
}
