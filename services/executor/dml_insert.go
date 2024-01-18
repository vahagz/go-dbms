package executor

import (
	"fmt"
	"io"

	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (es *ExecutorServiceT) dmlInsertValidate(q *dml.QueryInsert) error {
	table, ok := es.tables[q.Table]
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

				casted, err := q.Values[j][i].Cast(col.Typ, col.Meta)
				if err != nil {
					return errors.Wrapf(err, "failed to cast '%v' to type '%v'", q.Values[j][i].Value(), col.Typ)
				}
				q.Values[j][i] = casted
			}
		}
	}

	return nil
}

func (es *ExecutorServiceT) dmlInsert(q *dml.QueryInsert) (io.Reader, error) {
	if err := es.dmlInsertValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	for i, v := range q.Values {
		row := make(map[string]types.DataType, len(v))
		for j, col := range q.Columns {
			row[col] = v[j]
		}

		if err := es.tables[q.Table].Insert(row); err != nil {
			return nil, errors.Wrapf(err, "failed to insert into table, row: '%v'", i)
		}
	}

	return newPipe(EOS), nil
}
