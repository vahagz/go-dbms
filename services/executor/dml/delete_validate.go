package dml

import (
	"fmt"

	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (dml *DML) dmlDeleteValidate(q *dml.QueryDelete) error {
	table, ok := dml.Tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	if q.WhereIndex != nil {
		if !table.HasIndex(q.WhereIndex.Name) {
			return fmt.Errorf("index not found: '%s'", q.WhereIndex.Name)
		}

		fs := q.WhereIndex.FilterStart
		if fs != nil {
			for k, v := range fs.Value {
				casted, err := v.Cast(table.Column(k).Typ, table.Column(k).Meta)
				if err != nil {
					return errors.Wrapf(err, "failed to cast %v to %v", v.GetCode(), table.Column(k).Typ)
				}

				fs.Value[k] = casted
			}
		}
		
		fe := q.WhereIndex.FilterEnd
		if fe != nil {
			for k, v := range fe.Value {
				casted, err := v.Cast(table.Column(k).Typ, table.Column(k).Meta)
				if err != nil {
					return errors.Wrapf(err, "failed to cast %v to %v", v.GetCode(), table.Column(k).Typ)
				}

				fe.Value[k] = casted
			}
		}

		var validateWhere func(w *statement.WhereStatement)
		validateWhere = func (w *statement.WhereStatement) {
			if w == nil {
				return
			}

			if w.And != nil {
				for _, ws := range w.And {
					validateWhere(ws)
				}
			}
			if w.Or != nil {
				for _, ws := range w.Or {
					validateWhere(ws)
				}
			}
			if w.Statement != nil {
				w.Statement.Val = types.Type(table.Column(w.Statement.Col).Meta).Set(w.Statement.Val.Value())
			}
		}
		validateWhere((*statement.WhereStatement)(q.Where))
	}

	return nil
}
