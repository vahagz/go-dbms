package aggregator

import "go-dbms/pkg/types"

type AggregationMIN struct {
	Val types.DataType
}

func (as *AggregationMIN) Apply(value ...types.DataType) {
	if value[0].Compare("<", as.Val) {
		as.Val = value[0]
	}
}

func (as *AggregationMIN) Value() types.DataType {
	return as.Val
}
