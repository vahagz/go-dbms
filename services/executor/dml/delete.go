package dml

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (dml *DML) Delete(q *dml.QueryDelete) (io.WriterTo, error) {
	if err := dml.dmlDeleteValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]
	p := pipe.NewPipe(nil)

	go func() {
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
		if q.WhereIndex != nil {
			err = t.DeleteByIndex(
				q.WhereIndex.Name,
				q.WhereIndex.FilterStart,
				q.WhereIndex.FilterEnd,
				q.Where,
				process,
			)
		} else {
			err = t.Delete(q.Where, process)
		}

		if err != nil {
			panic(err)
		} else if _, err := p.Write(pipe.EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
