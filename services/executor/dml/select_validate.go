package dml

import (
	"fmt"

	"go-dbms/pkg/column"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/table"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/eval"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

func (dmlt *DML) dmlSelectValidate(q *dml.QuerySelect) (err error) {
	defer helpers.RecoverOnError(&err)()

	dmlt.validateFrom(q)
	if q.From.Type == dml.FROM_SCHEMA {
		dmlt.validateUseIndex(dmlt.Tables[q.From.Table], q)
		dmlt.validateWhereIndex(dmlt.Tables[q.From.Table], q.WhereIndex)
	}
	dmlt.validateProjections(q)
	dmlt.validateWhere(q.Where)
	dmlt.validateGroupBy(q)
	return nil
}

func (dmlt *DML) validateFrom(q *dml.QuerySelect) {
	if q.From.Type == dml.FROM_SCHEMA {
		if _, ok := dmlt.Tables[q.From.Table]; !ok {
			panic(fmt.Errorf("table not found: '%s'", q.From.Table))
		}
	} else if q.From.Type == dml.FROM_SUBQUERY {
		if err := dmlt.dmlSelectValidate(q.From.SubQuery.(*dml.QuerySelect)); err != nil {
			panic(err)
		}
	}
}

func (dmlt *DML) validateUseIndex(t table.ITable, q *dml.QuerySelect) {
	if q.UseIndex == "" {
		return
	} else if !t.HasIndex(q.UseIndex) {
		panic(fmt.Errorf("index not found: '%s'", q.UseIndex))
	}
}

func (dmlt *DML) validateProjections(q *dml.QuerySelect) {
	for i, pr := range q.Projections.Iterator() {
		dmlt.validateProjection(q, pr, i)
	}
}

func (dmlt *DML) validateProjection(
	q *dml.QuerySelect,
	p *projection.Projection,
	index int,
) {
	var t table.ITable
	var columns map[string]*column.Column
	if q.From.Type != dml.FROM_SUBQUERY {
		t = dmlt.Tables[q.From.Table]
		columns = t.ColumnsMap()
	}
	
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
				dmlt.validateProjection(q, pa, paIndex)
			}
		
		case projection.SUBQUERY:
			if err := dmlt.dmlSelectValidate(p.Subquery.(*dml.QuerySelect)); err != nil {
				panic(err)
			}
	}
}

func (dmlt *DML) validateWhereIndex(t table.ITable, wi *dml.WhereIndex) {
	if wi == nil {
		return
	}

	fs := wi.FilterStart
	if fs != nil {
		for _, cond := range fs.Conditions {
			col := t.Column(cond.Left.Alias)
			casted, err := eval.Eval(nil, cond.Right).Cast(col.Meta)
			if err != nil {
				panic(errors.Wrapf(err, "failed to cast %v to %v", col.Meta.GetCode(), col.Typ))
			}
	
			cond.Right.Type = projection.LITERAL
			cond.Right.Literal = casted
		}
	}

	fe := wi.FilterEnd
	if fe != nil {
		for _, cond := range fe.Conditions {
			col := t.Column(cond.Left.Alias)
			casted, err := eval.Eval(nil, cond.Right).Cast(col.Meta)
			if err != nil {
				panic(errors.Wrapf(err, "failed to cast %v to %v", col.Meta.GetCode(), col.Typ))
			}

			cond.Right.Type = projection.LITERAL
			cond.Right.Literal = casted
		}
	}
}

func (dmlt *DML) validateWhere(w *statement.WhereStatement) {
	// if w == nil {
	// 	return
	// }

	// if w.And != nil {
	// 	for _, ws := range w.And {
	// 		dmlt.validateWhere(ws)
	// 	}
	// }
	// if w.Or != nil {
	// 	for _, ws := range w.Or {
	// 		dmlt.validateWhere(ws)
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

func (dmlt *DML) validateGroupBy(q *dml.QuerySelect) {
	if q.From.Type == dml.FROM_SUBQUERY {
		return
	}

	t := dmlt.Tables[q.From.Table]
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
