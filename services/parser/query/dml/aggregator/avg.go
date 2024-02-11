package aggregator

import "go-dbms/pkg/types"

var avgMeta = &types.DataTypeINTEGERMeta{Signed: true, ByteSize: 8}

type AggregationAVG struct {
	*AggregatorBase
	Sum   int64
	Count uint64
}

func (as *AggregationAVG) Apply(row map[string]types.DataType) {
	val, err := row[as.Arguments[0].Alias].Cast(types.TYPE_INTEGER, avgMeta)
	if err != nil {
		panic(err)
	}
	as.Sum += val.Value().(int64)
	as.Count++
}

func (as *AggregationAVG) Value() types.DataType {
	var val float64
	if as.Count != 0 {
		val = float64(as.Sum) / float64(as.Count)
	}
	return types.Type(avgMeta).Set(val)
}
