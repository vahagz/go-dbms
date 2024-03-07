package function

import (
	"go-dbms/pkg/types"
)

const DIV FunctionType = "DIV"

func init() {
	functions[DIV] = func(row map[string]types.DataType, args []types.DataType) types.DataType {
		var err error
		var v1, v2 types.DataType
		if args[0].GetCode() == types.TYPE_FLOAT || args[1].GetCode() == types.TYPE_FLOAT {
			v1, err = args[0].Cast(floatMeta)
			if err != nil {
				panic(err)
			}

			v2, err = args[1].Cast(floatMeta)
			if err != nil {
				panic(err)
			}

			return types.Type(floatMeta).Set(v1.Value().(floatType) / v2.Value().(floatType))
		}

		v1, err = args[0].Cast(intMeta)
		if err != nil {
			panic(err)
		}

		v2, err = args[1].Cast(intMeta)
		if err != nil {
			panic(err)
		}

		return types.Type(intMeta).Set(v1.Value().(intType) / v2.Value().(intType))
	}
}
