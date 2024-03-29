package aggregator

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"
)

type AggregationMIN struct {
	*AggregatorBase
	Val types.DataType
}

func (as *AggregationMIN) Apply(row types.DataRow) {
	val := eval.Eval(row, as.Arguments[0])
	if as.Val == nil || val.CompareOp(types.Less, as.Val) {
		as.Val = val
	}
}

func (as *AggregationMIN) Value() types.DataType {
	return as.Val
}
