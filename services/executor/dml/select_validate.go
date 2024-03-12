package dml

import (
	"fmt"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/table"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/eval"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

func (dml *DML) dmlSelectValidate(q *dml.QuerySelect) (err error) {
	defer helpers.RecoverOnError(&err)()

	dml.validateFrom(q)
	dml.validateProjections(q)
	dml.validateWhereIndex(dml.Tables[q.Table], q.WhereIndex)
	dml.validateWhere(q.Where)
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

func (dml *DML) validateWhereIndex(t table.ITable, wi *dml.WhereIndex) {
	if wi == nil {
		return
	}

	if !t.HasIndex(wi.Name) {
		panic(fmt.Errorf("index not found: '%s'", wi.Name))
	}

	fs := wi.FilterStart
	if fs != nil {
		col := t.Column(fs.Left.Alias)
		casted, err := eval.Eval(nil, fs.Right).Cast(col.Meta)
		if err != nil {
			panic(errors.Wrapf(err, "failed to cast %v to %v", col.Meta.GetCode(), col.Typ))
		}

		fs.Right.Type = projection.LITERAL
		fs.Right.Literal = casted
	}

	fe := wi.FilterEnd
	if fe != nil {
		col := t.Column(fe.Left.Alias)
		casted, err := eval.Eval(nil, fe.Right).Cast(col.Meta)
		if err != nil {
			panic(errors.Wrapf(err, "failed to cast %v to %v", col.Meta.GetCode(), col.Typ))
		}

		fe.Right.Type = projection.LITERAL
		fe.Right.Literal = casted
	}
}

func (dml *DML) validateWhere(w *statement.WhereStatement) {
	// if w == nil {
	// 	return
	// }

	// if w.And != nil {
	// 	for _, ws := range w.And {
	// 		dml.validateWhere(ws)
	// 	}
	// }
	// if w.Or != nil {
	// 	for _, ws := range w.Or {
	// 		dml.validateWhere(ws)
	// 	}
	// }
	// if w.Statement != nil {
	// 	var err error
	// 	w.Statement.Right.Literal, err = w.Statement.Right.Literal.Cast(w.Statement.Left.Meta)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
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
