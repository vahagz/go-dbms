package projection

import (
	"fmt"

	"go-dbms/pkg/types"
)

type ProjectionType uint8

const (
	AGGREGATOR ProjectionType = iota
	FUNCTION
	IDENTIFIER
	LITERAL
)

type Projection struct {
	Alias     string
	Name      string
	Type      ProjectionType
	Arguments []*Projection
	Literal   types.DataType
}

func New() *Projections {
	return &Projections{
		mapping:        map[string]int{},
		list:           []*Projection{},
		aggregators:    []int{},
		nonAggregators: []int{},
	}
}

type Projections struct {
	mapping        map[string]int
	list           []*Projection
	aggregators    []int
	nonAggregators []int
}

func (p *Projections) Add(pr *Projection) {
	if _, ok := p.mapping[pr.Alias]; ok {
		panic(fmt.Errorf("projection with name '%s' already exists", pr.Alias))
	}

	p.list = append(p.list, pr)
	index := len(p.list) - 1
	p.mapping[pr.Alias] = index

	if pr.Type == AGGREGATOR {
		p.aggregators = append(p.aggregators, index)
	} else {
		p.nonAggregators = append(p.nonAggregators, index)
	}
}

func (p *Projections) Has(alias string) bool {
	_, found := p.mapping[alias]
	return found
}

func (p *Projections) GetByAlias(alias string) (pr *Projection, index int, found bool) {
	index, found = p.mapping[alias]
	return p.list[index], index, found
}

func (p *Projections) GetByIndex(index int) *Projection {
	return p.list[index]
}

func (p *Projections) Index(alias string) (int, bool) {
	index, found := p.mapping[alias]
	return index, found
}

func (p *Projections) Iterator() []*Projection {
	return p.list
}

func (p *Projections) Aggregators() []int {
	return p.aggregators
}

func (p *Projections) NonAggregators() []int {
	return p.nonAggregators
}
