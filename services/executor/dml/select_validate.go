package dml

import (
	"fmt"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

func (dml *DML) dmlSelectValidate(q *dml.QuerySelect) (err error) {
	defer helpers.RecoverOnError(&err)()

	dml.validateFrom(q)
	dml.validateProjections(q)
	dml.validateWhereIndex(q)
	dml.validateWhere(q, q.Where)
	dml.validateGroupBy(q)
	return nil
}

func (dml *DML) validateFrom(q *dml.QuerySelect) {
	if _, ok := dml.Tables[q.Table]; !ok {
		panic(fmt.Errorf("table not found: '%s'", q.Table))
	}
}

func (dml *DML) validateProjections(q *dml.QuerySelect) {
	for i, pr := range q.Projections.Iterator() {
		dml.validateProjection(q, pr, i)
	}
}

func (dml *DML) validateProjection(
	q *dml.QuerySelect,
	p *projection.Projection,
	index int,
) {
	t := dml.Tables[q.Table]
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
				dml.validateProjection(q, pa, paIndex)
			}
	}
}

func (dml *DML) validateWhereIndex(q *dml.QuerySelect) {
	t := dml.Tables[q.Table]

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

func (dml *DML) validateWhere(q *dml.QuerySelect, w *statement.WhereStatement) {
	if w == nil {
		return
	}

	t := dml.Tables[q.Table]

	if w.And != nil {
		for _, ws := range w.And {
			dml.validateWhere(q, ws)
		}
	}
	if w.Or != nil {
		for _, ws := range w.Or {
			dml.validateWhere(q, ws)
		}
	}
	if w.Statement != nil {
		w.Statement.Val = types.Type(t.Column(w.Statement.Col).Meta).Set(w.Statement.Val.Value())
	}
}

func (dml *DML) validateGroupBy(q *dml.QuerySelect) {
	t := dml.Tables[q.Table]
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
