package aggregator

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/eval"
	"go-dbms/services/parser/query/dml/function"
)

type AggregationSUM struct {
	*AggregatorBase
	Sum  types.DataType
}

func (as *AggregationSUM) Apply(row types.DataRow) {
	val := eval.Eval(row, as.Arguments[0])
	if as.Sum == nil {
		as.Sum = val
	} else {
		as.Sum = function.Eval(function.ADD, row, []types.DataType{as.Sum, val})
	}
}

func (as *AggregationSUM) Value() types.DataType {
	return as.Sum
}
