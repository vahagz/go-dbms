package aggregator

import (
	"errors"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/projection"

	"golang.org/x/exp/constraints"
)

type numeric interface {
	constraints.Float | constraints.Integer
}

type comparable interface {
	constraints.Ordered
}

type AggregatorType string

const (
	SUM      AggregatorType = "SUM"
	MAX      AggregatorType = "MAX"
	MIN      AggregatorType = "MIN"
	AVG      AggregatorType = "AVG"
	COUNT    AggregatorType = "COUNT"
	ANYLAST  AggregatorType = "ANYLAST"
	ANYFIRST AggregatorType = "ANYFIRST"
)

var aggregators = map[AggregatorType]struct{}{
	SUM:      {},
	MAX:      {},
	MIN:      {},
	AVG:      {},
	COUNT:    {},
	ANYLAST:  {},
	ANYFIRST: {},
}

type AggregatorBase struct {
	Arguments []*projection.Projection
}

func (ab *AggregatorBase) Value() types.DataType {
	panic(errors.New("unimplemented"))
}

func (ab *AggregatorBase) Apply(row types.DataRow) {
	panic(errors.New("unimplemented"))
}

type Aggregator interface {
	Value() types.DataType
	Apply(row types.DataRow)
}

func IsAggregator(fn string) bool {
	_, ok := aggregators[AggregatorType(fn)]
	return ok
}

func New(name AggregatorType, args []*projection.Projection) Aggregator {
	ab := &AggregatorBase{args}
	switch name {
		case AVG:      return &AggregationAVG{AggregatorBase: ab}
		case COUNT:    return &AggregationCOUNT{AggregatorBase: ab}
		case MAX:      return &AggregationMAX{AggregatorBase: ab}
		case MIN:      return &AggregationMIN{AggregatorBase: ab}
		case SUM:      return &AggregationSUM{AggregatorBase: ab}
		case ANYLAST:  return &AggregationANYLAST{AggregatorBase: ab}
		case ANYFIRST: return &AggregationANYFIRST{AggregatorBase: ab}
		default:       panic(errors.New("unknown aggregate function"))
	}
}
