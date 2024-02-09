package aggregator

import "go-dbms/pkg/types"

type AggregationMIN struct {
	*AggregatorBase
	Val types.DataType
}

func (as *AggregationMIN) Apply(row map[string]types.DataType) {
	if row[as.Arguments[0]].Compare("<", as.Val) {
		as.Val = row[as.Arguments[0]]
	}
}

func (as *AggregationMIN) Value() types.DataType {
	return as.Val
}
