package aggregator

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"
)

type AggregationMAX struct {
	*AggregatorBase
	Val types.DataType
}

func (as *AggregationMAX) Apply(row map[string]types.DataType) {
	val := eval.Eval(row, as.Arguments[0])
	if as.Val == nil || val.Compare(types.Greater, as.Val) {
		as.Val = val
	}
}

func (as *AggregationMAX) Value() types.DataType {
	return as.Val
}
