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

func (dml *DML) Delete(q *dml.QueryDelete, es parent.Executor) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	if err := dml.dmlDeleteValidate(q); err != nil {
		return nil, nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]
	dst := stream.New[types.DataRow](1)

	go func() {
		process := func(s stream.Reader[types.DataRow]) error {
			defer dst.Close()
			for row, ok := s.Pop(); ok; row, ok = s.Pop() {
				dst.Push(row)
				dst.ShouldContinue() // have no effect but must call because return type is stream.ReaderContinue
			}
			return nil
		}

		var s stream.Reader[types.DataRow]
		if q.WhereIndex != nil {
			s = helpers.MustVal(t.DeleteByIndex(
				q.UseIndex,
				q.WhereIndex.FilterStart,
				q.WhereIndex.FilterEnd,
				q.Where,
			))
		} else {
			s = t.Delete(q.Where)
		}

		helpers.Must(process(s))
	}()

	return dst, projection.FromCols(t.PrimaryColumns()), nil
}
