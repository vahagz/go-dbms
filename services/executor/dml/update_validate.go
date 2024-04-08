package dml

import (
	"fmt"

	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (dml *DML) dmlUpdateValidate(q *dml.QueryUpdate) error {
	table, ok := dml.Tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	columns := table.ColumnsMap()
	for colName, v := range q.Values {
		if col, ok := columns[colName]; !ok {
			return fmt.Errorf("column not found: '%s'", colName)
		} else {
			casted, err := v.Cast(col.Meta)
			if err != nil {
				return errors.Wrapf(err, "failed to cast %v to %v", v.GetCode(), col.Typ)
			}

			q.Values[colName] = casted
		}
	}

	dml.validateWhereIndex(table, q.WhereIndex)
	dml.validateWhere(q.Where)

	return nil
}
