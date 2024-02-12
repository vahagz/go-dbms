package function

import "go-dbms/pkg/types"

type FunctionADD struct {
	*FunctionBase
}

func (f *FunctionADD) Apply(row map[string]types.DataType) types.DataType {
	var val intType

	for _, p := range f.Arguments {
		v, err := row[p.Alias].Cast(intCode, intMeta)
		if err != nil {
			panic(err)
		}

		val += v.Value().(intType)
	}

	return types.Type(intMeta).Set(val)
}
