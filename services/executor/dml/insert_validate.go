package dml

import (
	"fmt"

	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (dml *DML) dmlInsertValidate(q *dml.QueryInsert) error {
	table, ok := dml.Tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	columns := table.ColumnsMap()
	for i, colName := range q.Columns {
		if col, ok := columns[colName]; !ok {
			return fmt.Errorf("column not found: '%s'", colName)
		} else {
			for j := 0; j < len(q.Values); j++ {
				if len(q.Values[j]) != len(q.Columns) {
					return fmt.Errorf(
						"count of values on row %v is %v, must be %v",
						j, len(q.Values[j]), len(q.Columns),
					)
				}

				casted, err := q.Values[j][i].Cast(col.Meta)
				if err != nil {
					return errors.Wrapf(err, "failed to cast '%v' to type '%v'", q.Values[j][i].Value(), col.Typ)
				}
				q.Values[j][i] = casted
			}
		}
	}

	return nil
}
