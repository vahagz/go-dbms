package dml

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (dml *DML) Insert(q *dml.QueryInsert) (io.WriterTo, error) {
	if err := dml.dmlInsertValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]

	p := pipe.NewPipe(nil)
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

		if _, err := p.Write(pipe.EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
