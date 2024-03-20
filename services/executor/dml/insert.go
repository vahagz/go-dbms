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

func (dml *DML) Insert(q *dml.QueryInsert) (io.WriterTo, error) {
	if err := dml.dmlInsertValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]

	p := pipe.NewPipe(nil)
	go func ()  {
		pCols := t.PrimaryColumns()
		record := make([]interface{}, len(pCols))
		in := stream.New[types.DataRow](1)
		out, eg := t.Insert(in)

		eg.Go(func() error {
			defer in.Close()
			for _, v := range q.Values {
				row := types.DataRow{}
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
