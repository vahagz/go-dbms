package aggregator

import "go-dbms/pkg/types"

type AggregationMAX struct {
	*AggregatorBase
	Val types.DataType
}

func (as *AggregationMAX) Apply(row map[string]types.DataType) {
	if row[as.Arguments[0].Alias].Compare(">", as.Val) {
		as.Val = row[as.Arguments[0].Alias]
	}
}

func (as *AggregationMAX) Value() types.DataType {
	return as.Val
}
