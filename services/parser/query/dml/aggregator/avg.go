package aggregator

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"
)

var float64Meta = &types.DataTypeFLOATMeta{ByteSize: 8}

type AggregationAVG struct {
	*AggregatorBase
	Sum   float64
	Count uint64
}

func (as *AggregationAVG) Apply(row map[string]types.DataType) {
	val, err := eval.Eval(row, as.Arguments[0]).Cast(float64Meta)
	if err != nil {
		panic(err)
	}
	as.Sum += val.Value().(float64)
	as.Count++
}

func (as *AggregationAVG) Value() types.DataType {
	var val float64
	if as.Count != 0 {
		val = float64(as.Sum) / float64(as.Count)
	}
	return types.Type(float64Meta).Set(val)
}
