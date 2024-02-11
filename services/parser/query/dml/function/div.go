package function

import "go-dbms/pkg/types"

type FunctionDIV struct {
	*FunctionBase
}

func (f *FunctionDIV) Apply(value ...types.DataType) types.DataType {
	v1, err := value[0].Cast(intCode, intMeta)
	if err != nil {
		panic(err)
	}

	v2, err := value[1].Cast(intCode, intMeta)
	if err != nil {
		panic(err)
	}

	return types.Type(intMeta).Set(v1.Value().(intType) / v2.Value().(intType))
}
