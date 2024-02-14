package aggregator

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"
)

type AggregationSUM[T numeric] struct {
	*AggregatorBase
	Sum T
	Meta types.DataTypeMeta
}

func (as *AggregationSUM[T]) Apply(row map[string]types.DataType) {
	val, err := eval.Eval(row, as.Arguments[0]).Cast(as.Meta)
	if err != nil {
		panic(err)
	}
	as.Sum += val.Value().(T)
}

func (as *AggregationSUM[T]) Value() types.DataType {
	return types.Type(as.Meta).Set(as.Sum)
}
