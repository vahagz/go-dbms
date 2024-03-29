package dml

import (
	"go-dbms/pkg/types"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (dml *DML) Insert(q *dml.QueryInsert, es parent.Executor) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	if err := dml.dmlInsertValidate(q); err != nil {
		return nil, nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]
	dst := stream.New[types.DataRow](1)
	in := stream.New[types.DataRow](1)
	out, eg := t.Insert(in)

	go func ()  {

		eg.Go(func() error {
			defer in.Close()
			defer dst.Close()

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

				dst.Push(pk)
				dst.ShouldContinue() // have no effect but must call because return type is stream.ReaderContinue
			}
			return nil
		})

		if err := eg.Wait(); err != nil {
			panic(err)
		}
	}()

	return dst, projection.FromCols(t.PrimaryColumns()), nil
}
