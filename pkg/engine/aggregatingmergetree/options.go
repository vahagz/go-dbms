package aggregatingmergetree

import (
	"go-dbms/pkg/table"
	"go-dbms/services/parser/query/dml/aggregator"
)

type Options struct {
	*table.Options
	Aggregations map[string]aggregator.AggregatorType
}
