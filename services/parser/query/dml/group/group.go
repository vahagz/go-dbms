package group

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/aggregator"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/stream"
)

type subGroup struct {
	next       map[string]*subGroup
	val        map[string]aggregator.Aggregator
	groupItems types.DataRow
}

type Group struct {
	projections *projection.Projections
	groupList   map[string]struct{}
	groups      *subGroup
	dst         stream.Writer[types.DataRow]
}

func New(projections *projection.Projections, groupList map[string]struct{}, dst stream.Writer[types.DataRow]) *Group {
	return &Group{
		projections: projections,
		groupList:   groupList,
		groups:      &subGroup{},
		dst:         dst,
	}
}

func (g *Group) Add(row types.DataRow) {
	gr := g.groups
	groupItems := gr.groupItems
	for gIdx := range g.projections.NonAggregators() {
		gi := g.projections.GetByIndex(gIdx).Alias
		if gr.next == nil {
			gr.next = map[string]*subGroup{}
		}

		key := string(row[gi].Bytes())
		if next, ok := gr.next[key]; !ok {
			if groupItems == nil {
				groupItems = types.DataRow{gi: row[gi]}
			} else {
				groupItems[gi] = row[gi]
			}
			sg := &subGroup{}
			gr.next[key] = sg
			gr = sg

			cp := types.DataRow{}
			for k, v := range groupItems {
				cp[k] = v
			}
			sg.groupItems = cp
		} else {
			gr = next
			groupItems = next.groupItems
		}
	}
	if gr.val == nil {
		gr.val = map[string]aggregator.Aggregator{}
	}

	for _, i := range g.projections.Aggregators() {
		p := g.projections.GetByIndex(i)
		var aggr aggregator.Aggregator
		if ag, ok := gr.val[p.Alias]; !ok {
			aggr = aggregator.New(aggregator.AggregatorType(p.Name), p.Arguments)
			gr.val[p.Alias] = aggr
		} else {
			aggr = ag
		}

		aggr.Apply(row)
	}
}

func (g *Group) Flush() (n int, err error) {
	return g.flush(g.groups)
}

func (g *Group) flush(gr *subGroup) (n int, err error) {
	if gr.next != nil {
		for _, sg := range gr.next {
			sN, sErr := g.flush(sg)
			n += sN
			if sErr != nil {
				return 0, sErr
			}
		}
		return n, err
	}

	if len(gr.val) == 0 {
		return 0, nil
	}

	prList := g.projections.Iterator()
	record := make(types.DataRow, len(prList))

	for _, pr := range prList {
		var val types.DataType
		if pr.Type == projection.AGGREGATOR {
			val = gr.val[pr.Alias].Value()
		} else {
			val = gr.groupItems[pr.Alias]
		}
		record[pr.Alias] = val
	}

	g.dst.Push(record)
	return 0, nil
}
