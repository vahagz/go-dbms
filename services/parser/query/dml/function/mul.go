package function

import "go-dbms/pkg/types"

type FunctionMUL struct {
	*FunctionBase
}

func (f *FunctionMUL) Apply(row map[string]types.DataType) types.DataType {
	var val intType

	for _, p := range f.Arguments {
		v, err := row[p.Alias].Cast(intCode, intMeta)
		if err != nil {
			panic(err)
		}

		val *= v.Value().(intType)
	}

	return types.Type(intMeta).Set(val)
}
