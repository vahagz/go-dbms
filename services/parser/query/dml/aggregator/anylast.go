package aggregator

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"
)

type AggregationANYLAST struct {
	*AggregatorBase
	Val types.DataType
}

func (as *AggregationANYLAST) Apply(row types.DataRow) {
	as.Val = eval.Eval(row, as.Arguments[0])
}

func (as *AggregationANYLAST) Value() types.DataType {
	return as.Val
}
