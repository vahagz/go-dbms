package dml

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/pkg/table"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func (dml *DML) Insert(q *dml.QueryInsert) (io.WriterTo, error) {
	if err := dml.dmlInsertValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]

	p := pipe.NewPipe(nil)
	go func ()  {
		pCols := t.PrimaryColumns()
		record := make([]interface{}, len(pCols))
		in, out := stream.New[table.DataRow](0), stream.New[table.DataRow](0)
		eg := &errgroup.Group{}

		eg.Go(func() error {
			return t.Insert(in, out)
		})

		eg.Go(func() error {
			for _, v := range q.Values {
				row := map[string]types.DataType{}
				for j, col := range q.Columns {
					row[col] = v[j]
				}

				in.Push(row)
				pk, ok := out.Pop()
				if !ok {
					return errors.New("unexpected error while reading insertion result")
				}

				for i, col := range pCols {
					record[i] = pk[col.Name].Value()
				}

				if _, err := p.Write(helpers.MustVal(json.Marshal(record))); err != nil {
					return errors.Wrap(err, "failed to push marshaled record")
				}
			}

			in.Close()
			return nil
		})

		if err := eg.Wait(); err != nil {
			panic(err)
		} else if _, err := p.Write(pipe.EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
