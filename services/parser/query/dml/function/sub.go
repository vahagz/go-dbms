package function

import "go-dbms/pkg/types"

type FunctionSUB struct {
	*FunctionBase
}

func (f *FunctionSUB) Apply(row map[string]types.DataType) types.DataType {
	v1, err := row[f.Arguments[0].Alias].Cast(intCode, intMeta)
	if err != nil {
		panic(err)
	}

	v2, err := row[f.Arguments[1].Alias].Cast(intCode, intMeta)
	if err != nil {
		panic(err)
	}

	return types.Type(intMeta).Set(v1.Value().(intType) - v2.Value().(intType))
}
