package executor

import (
	"encoding/json"
	"fmt"
	"io"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/group"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

func (es *ExecutorServiceT) dmlSelectValidate(q *dml.QuerySelect) (err error) {
	defer helpers.RecoverOnError(&err)()

	es.validateFrom(q)
	es.validateProjections(q)
	es.validateWhereIndex(q)
	es.validateWhere(q, q.Where)
	es.validateGroupBy(q)
	return nil
}

func (es *ExecutorServiceT) validateFrom(q *dml.QuerySelect) {
	if _, ok := es.tables[q.Table]; !ok {
		panic(fmt.Errorf("table not found: '%s'", q.Table))
	}
}

func (es *ExecutorServiceT) validateProjections(q *dml.QuerySelect) {
	for i, pr := range q.Projections.Iterator() {
		es.validateProjection(q, pr, i)
	}
}

func (es *ExecutorServiceT) validateProjection(
	q *dml.QuerySelect,
	p *projection.Projection,
	index int,
) {
	t := es.tables[q.Table]
	columns := t.ColumnsMap()
	
	switch p.Type {
		case projection.IDENTIFIER:
			isAlias := q.Projections.Has(p.Name)
			_, isColumn := columns[p.Name]
			if !isAlias && !isColumn {
				panic(fmt.Errorf("identifier not found: '%s'", p.Name))
			}

		case projection.LITERAL: break // do nothing

		case projection.AGGREGATOR, projection.FUNCTION:
			for _, pa := range p.Arguments {
				_, isColumn := columns[pa.Name]
				paIndex, found := q.Projections.Index(pa.Alias)
				if !isColumn && found && index <= paIndex {
					panic(fmt.Errorf("projection '%s' is defined after '%s'", pa.Alias, p.Alias))
				}
				es.validateProjection(q, pa, paIndex)
			}
	}
}

func (es *ExecutorServiceT) validateWhereIndex(q *dml.QuerySelect) {
	t := es.tables[q.Table]

	if q.WhereIndex != nil {
		if !t.HasIndex(q.WhereIndex.Name) {
			panic(fmt.Errorf("index not found: '%s'", q.WhereIndex.Name))
		}

		fs := q.WhereIndex.FilterStart
		if fs != nil {
			for k, v := range fs.Value {
				casted, err := v.Cast(t.Column(k).Typ, t.Column(k).Meta)
				if err != nil {
					panic(errors.Wrapf(err, "failed to cast %v to %v", v.GetCode(), t.Column(k).Typ))
				}

				fs.Value[k] = casted
			}
		}
		
		fe := q.WhereIndex.FilterEnd
		if fe != nil {
			for k, v := range fe.Value {
				casted, err := v.Cast(t.Column(k).Typ, t.Column(k).Meta)
				if err != nil {
					panic(errors.Wrapf(err, "failed to cast %v to %v", v.GetCode(), t.Column(k).Typ))
				}

				fe.Value[k] = casted
			}
		}
	}
}

func (es *ExecutorServiceT) validateWhere(q *dml.QuerySelect, w *statement.WhereStatement) {
	if w == nil {
		return
	}

	t := es.tables[q.Table]

	if w.And != nil {
		for _, ws := range w.And {
			es.validateWhere(q, ws)
		}
	}
	if w.Or != nil {
		for _, ws := range w.Or {
			es.validateWhere(q, ws)
		}
	}
	if w.Statement != nil {
		w.Statement.Val = types.Type(t.Column(w.Statement.Col).Meta).Set(w.Statement.Val.Value())
	}
}

func (es *ExecutorServiceT) validateGroupBy(q *dml.QuerySelect) {
	t := es.tables[q.Table]
	columns := t.ColumnsMap()

	for groupItem := range q.GroupBy {
		if pr, _, found := q.Projections.GetByAlias(groupItem); found {
			if pr.Type == projection.AGGREGATOR {
				panic(fmt.Errorf("can't group by aggregator:'%s'", pr.Alias))
			}
		} else {
			_, isColumn := columns[groupItem]
			if !isColumn {
				panic(fmt.Errorf("unknown group item:'%s'", groupItem))
			}
		}
	}

	for _, p := range q.Projections.Iterator() {
		if p.Type != projection.AGGREGATOR {
			if _, ok := q.GroupBy[p.Alias]; !ok {
				if q.GroupBy == nil {
					q.GroupBy = map[string]struct{}{}
				}
				q.GroupBy[p.Alias] = struct{}{}
			}
		}
	}
}

func (es *ExecutorServiceT) dmlSelect(q *dml.QuerySelect) (io.WriterTo, error) {
	if err := es.dmlSelectValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := es.tables[q.Table]
	p := newPipe(nil)

	var gr *group.Group
	if len(q.Projections.Aggregators()) != 0 {
		gr = group.New(q.Projections, q.GroupBy, p)
	}

	go func() {
		var (
			name string
			indexFilterStart, indexFilterEnd *index.Filter
			filter *statement.WhereStatement
		)

		if q.WhereIndex != nil {
			name = q.WhereIndex.Name
			if q.WhereIndex.FilterStart != nil {
				indexFilterStart = &index.Filter{
					Operator: q.WhereIndex.FilterStart.Operator,
					Value:    q.WhereIndex.FilterStart.Value,
				}

				if q.WhereIndex.FilterEnd != nil {
					indexFilterEnd = &index.Filter{
						Operator: q.WhereIndex.FilterEnd.Operator,
						Value:    q.WhereIndex.FilterEnd.Value,
					}
				}
			}
		}
		if q.Where != nil {
			filter = (*statement.WhereStatement)(q.Where)
		}

		prList := q.Projections.Iterator()
		record := make([]interface{}, 0, len(prList))

		process := func(row map[string]types.DataType) (bool, error) {
			if gr != nil {			
				gr.Add(row)
				return false, nil
			}

			clear(record)
			for _, pr := range prList {
				record = append(record, row[pr.Name].Value())
			}

			blob, err := json.Marshal(record)
			if err != nil {
				return true, errors.Wrap(err, "failed to marshal record")
			}

			_, err = p.Write(blob)
			if err != nil {			
				return true, errors.Wrap(err, "failed to push marshaled record")
			}
			return false, nil
		}

		var err error
		if indexFilterStart != nil {
			err = t.ScanByIndex(name, indexFilterStart, indexFilterEnd, filter, process)
		} else {
			err = t.FullScanByIndex(t.PrimaryKey(), false, filter, process)
		}

		if gr != nil {
			gr.Flush()
		}

		if err != nil {
			panic(err)
		} else if _, err := p.Write(EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
