package function

import "go-dbms/pkg/types"

type FunctionMUL struct {
	*FunctionBase
}

func (f *FunctionMUL) Apply(value ...types.DataType) types.DataType {
	var val intType

	for _, dt := range value {
		v, err := dt.Cast(intCode, intMeta)
		if err != nil {
			panic(err)
		}

		val *= v.Value().(intType)
	}

	return types.Type(intMeta).Set(val)
}
