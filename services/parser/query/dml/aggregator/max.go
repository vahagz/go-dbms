package aggregator

import "go-dbms/pkg/types"

type AggregationMAX struct {
	Val types.DataType
}

func (as *AggregationMAX) Apply(value ...types.DataType) {
	if value[0].Compare(">", as.Val) {
		as.Val = value[0]
	}
}

func (as *AggregationMAX) Value() types.DataType {
	return as.Val
}
