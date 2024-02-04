package executor

import (
	"encoding/json"
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

func (es *ExecutorServiceT) dmlInsert(q *dml.QueryInsert) (io.WriterTo, error) {
	if err := es.dmlInsertValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := es.tables[q.Table]

	p := newPipe(nil)
	go func ()  {
		columns := t.PrimaryColumns()

		for i, v := range q.Values {
			row := make(map[string]types.DataType, len(v))
			for j, col := range q.Columns {
				row[col] = v[j]
			}
	
			if pk, err := t.Insert(row); err != nil {
				panic(errors.Wrapf(err, "failed to insert into table, row: '%v'", i))
			} else {
				record := make([]interface{}, 0, len(columns))
				for _, col := range columns {
					record = append(record, pk[col.Name].Value())
				}

				blob, err := json.Marshal(record)
				if err != nil {
					panic(errors.Wrap(err, "failed to marshal record"))
				}

				_, err = p.Write(blob)
				if err != nil {
					panic(errors.Wrap(err, "failed to push marshaled record"))
				}
			}
		}

		/*if err := p.bw.Flush(); err != nil {
			panic(err)
		} else*/ if _, err := p.Write(EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
