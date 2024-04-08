package aggregatingmergetree

import (
	"go-dbms/pkg/table"
	"go-dbms/services/parser/query/dml/aggregator"
)

type Metadata struct {
	*table.Metadata
	Aggregations map[string]aggregator.AggregatorType `json:"aggregations"`
}

type IMetadata interface {
	table.IMetadata
	GetAggregations() map[string]aggregator.AggregatorType
	SetAggregations(v map[string]aggregator.AggregatorType)
}

func (m *Metadata) GetAggregations() map[string]aggregator.AggregatorType { return m.Aggregations }
func (m *Metadata) SetAggregations(v map[string]aggregator.AggregatorType) { m.Aggregations = v }
