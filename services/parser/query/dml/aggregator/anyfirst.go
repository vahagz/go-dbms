package aggregator

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"
)

type AggregationANYFIRST struct {
	*AggregatorBase
	Val types.DataType
}

func (as *AggregationANYFIRST) Apply(row types.DataRow) {
	if as.Val == nil {
		as.Val = eval.Eval(row, as.Arguments[0])
	}
}

func (as *AggregationANYFIRST) Value() types.DataType {
	return as.Val
}
