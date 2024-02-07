package projection

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/aggregator"
)

type Projection struct {
	Name              *string
	Column            *string
	Arguments         []string
	AggregateFunction aggregator.Aggregator
	Value             types.DataType
}
