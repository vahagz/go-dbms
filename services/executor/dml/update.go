package dml

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/index"
	"go-dbms/pkg/pipe"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (dml *DML) Update(q *dml.QueryUpdate) (io.WriterTo, error) {
	if err := dml.dmlUpdateValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]
	p := pipe.NewPipe(nil)

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

		columns := t.PrimaryColumns()
		process := func(row map[string]types.DataType) error {
			record := make([]interface{}, 0, len(columns))
			for _, col := range columns {
				record = append(record, row[col.Name].Value())
			}

			blob, err := json.Marshal(record)
			if err != nil {
				return errors.Wrap(err, "failed to marshal record")
			}

			_, err = p.Write(blob)
			return errors.Wrap(err, "failed to push marshaled record")
		}

		var err error
		if indexFilterStart != nil {
			err = t.UpdateByIndex(name, indexFilterStart, indexFilterEnd, filter, q.Values, process)
		} else {
			err = t.Update(filter, q.Values, process)
		}

		if err != nil {
			panic(err)
		} else if _, err := p.Write(pipe.EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
