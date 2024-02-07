package aggregator

import "go-dbms/pkg/types"

type AggregationSUM[T numeric] struct {
	Sum T
	Meta types.DataTypeMeta
}

func (as *AggregationSUM[T]) Apply(value ...types.DataType) {
	val, err := value[0].Cast(as.Meta.GetCode(), as.Meta)
	if err != nil {
		panic(err)
	}
	as.Sum += val.Value().(T)
}

func (as *AggregationSUM[T]) Value() types.DataType {
	return types.Type(as.Meta).Set(as.Sum)
}
