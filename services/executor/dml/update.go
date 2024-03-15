package dml

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (dml *DML) Update(q *dml.QueryUpdate) (io.WriterTo, error) {
	if err := dml.dmlUpdateValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]
	p := pipe.NewPipe(nil)

	go func() {
		columns := t.PrimaryColumns()
		process := func(s stream.Reader[types.DataRow]) error {
			for row, ok := s.Pop(); ok; row, ok = s.Pop() {
				record := make([]interface{}, 0, len(columns))
				for _, col := range columns {
					record = append(record, row[col.Name].Value())
				}

				if _, err := p.Write(helpers.MustVal(json.Marshal(record))); err != nil {
					return errors.Wrap(err, "failed to push marshaled record")
				}
			}
			return nil
		}

		var s stream.Reader[types.DataRow]
		if q.WhereIndex != nil {
			s = helpers.MustVal(t.UpdateByIndex(
				q.WhereIndex.Name,
				q.WhereIndex.FilterStart,
				q.WhereIndex.FilterEnd,
				q.Where,
				q.Values,
			))
		} else {
			s = t.Update(q.Where, q.Values)
		}

		helpers.Must(process(s))
		helpers.MustVal(p.Write(pipe.EOS))
	}()

	return p, nil
}
