package aggregator

import (
	"errors"

	"go-dbms/pkg/types"

	"golang.org/x/exp/constraints"
)

type numeric interface {
	constraints.Float | constraints.Integer
}

type comparable interface {
	constraints.Ordered
}

const (
	SUM = "SUM"
	MAX = "MAX"
	MIN = "MIN"
	AVG = "AVG"
	COUNT = "COUNT"
)

type Aggregator interface {
	Value() types.DataType
	Apply(value ...types.DataType)
}

func New(name string, code types.TypeCode) Aggregator {
	switch name {
		case AVG: {
			if !types.IsNumeric(code) {
				panic(errors.New("unsupported column type"))
			}
			return &AggregationAVG{}
		}
		case COUNT: {
			return &AggregationCOUNT{}
		}
		case MAX: {
			return &AggregationMAX{}
		}
		case MIN: {
			return &AggregationMIN{}
		}
		case SUM: {
			if !types.IsNumeric(code) {
				panic(errors.New("unsupported column type"))
			}
			return &AggregationSUM[int64]{Meta: &types.DataTypeINTEGERMeta{Signed: true, ByteSize: 8}}
		}
		default: {
			panic(errors.New("unknown aggregate function"))
		}
	}
}
