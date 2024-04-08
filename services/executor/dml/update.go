package dml

import (
	"go-dbms/pkg/types"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (dml *DML) Update(q *dml.QueryUpdate, es parent.Executor) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	if err := dml.dmlUpdateValidate(q); err != nil {
		return nil, nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]
	dst := stream.New[types.DataRow](1)

	go func() {
		defer dst.Close()

		process := func(s stream.Reader[types.DataRow]) error {
			for row, ok := s.Pop(); ok; row, ok = s.Pop() {
				dst.Push(row)
				dst.ShouldContinue() // have no effect but must call because return type is stream.ReaderContinue
			}
			return nil
		}

		var s stream.Reader[types.DataRow]
		if q.WhereIndex != nil {
			s = helpers.MustVal(t.UpdateByIndex(
				q.UseIndex,
				q.WhereIndex.FilterStart,
				q.WhereIndex.FilterEnd,
				q.Where,
				q.Values,
			))
		} else {
			s = t.Update(q.Where, q.Values)
		}

		helpers.Must(process(s))
	}()

	return dst, projection.FromCols(t.PrimaryColumns()), nil
}
