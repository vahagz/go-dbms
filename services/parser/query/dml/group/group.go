package group

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml/aggregator"
	"go-dbms/services/parser/query/dml/projection"

	"github.com/pkg/errors"
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
	dst         io.Writer
}

func New(projections *projection.Projections, groupList map[string]struct{}, dst io.Writer) *Group {
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
	for gi := range g.groupList {
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
			sg := &subGroup{groupItems: groupItems}
			gr.next[key] = sg
			gr = sg
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
	record := make([]interface{}, 0, len(prList))

	for _, pr := range prList {
		var val types.DataType
		if pr.Type == projection.AGGREGATOR {
			val = gr.val[pr.Alias].Value()
		} else {
			val = gr.groupItems[pr.Alias]
		}

		record = append(record, val.Value())
	}

	blob, err := json.Marshal(record)
	if err != nil {
		return 0, errors.Wrap(err, "failed to marshal record")
	}

	_, err = g.dst.Write(blob)
	if err != nil {			
		return 0, errors.Wrap(err, "failed to push marshaled record")
	}

	return 0, nil
}
