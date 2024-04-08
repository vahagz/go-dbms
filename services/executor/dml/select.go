package dml

import (
	"cmp"

	"go-dbms/pkg/types"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/eval"
	"go-dbms/services/parser/query/dml/group"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (dmlt *DML) Select(q *dml.QuerySelect, es parent.Executor) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	if err := dmlt.dmlSelectValidate(q); err != nil {
		return nil, nil, errors.Wrapf(err, "validation error")
	}

	dst := stream.New[types.DataRow](1)

	var gr *group.Group
	if len(q.Projections.Aggregators()) != 0 {
		gr = group.New(q.Projections, q.GroupBy, dst)
	}

	go func() {
		defer dst.Close()

		nonAggr := q.Projections.NonAggregators()
		prList := q.Projections.Iterator()

		for _, pr := range prList {
			if pr.Type == projection.SUBQUERY {
				r, p, err := es.Exec(pr.Subquery)
				if err != nil {
					panic(err)
				}

				row, _ := r.Pop()
				r.Continue(false)
				var val types.DataType
				if len(row) == 0 {
					val = types.Type(types.Meta(types.TYPE_STRING)).Set("")
				} else {
					val = row[p.GetByIndex(0).Alias]
				}

				pr.Type = projection.LITERAL
				pr.Literal = val
			}
		}

		process := func(s stream.ReaderContinue[types.DataRow]) error {
			for row, ok := s.Pop(); ok; row, ok = s.Pop() {
				s.Continue(true)
				if gr == nil {
					for _, p := range prList {
						val := eval.Eval(row, p)
						row[p.Alias] = val
					}
				} else {
					for _, i := range nonAggr {
						p := q.Projections.GetByIndex(i)
						row[p.Alias] = eval.Eval(row, p)
					}
				}

				if q.Where != nil && !q.Where.Compare(row) {
					continue
				} else if gr != nil {
					gr.Add(row)
					continue
				}

				dst.Push(row)
				if !dst.ShouldContinue() {
					s.Pop()
					s.Continue(false)
				}
			}
			return nil
		}

		var s stream.ReaderContinue[types.DataRow]
		if q.From.Type == dml.FROM_SUBQUERY {
			var err error
			s, _, err = es.Exec(q.From.SubQuery)
			if err != nil {
				panic(err)
			}
		} else {
			t := dmlt.Tables[q.From.Table]
			if q.WhereIndex != nil {
				s = helpers.MustVal(t.ScanByIndex(
					q.UseIndex,
					q.WhereIndex.FilterStart,
					q.WhereIndex.FilterEnd,
				))
			} else {
				s = helpers.MustVal(t.FullScanByIndex(cmp.Or(q.UseIndex, t.PrimaryKey()), false))
			}
		}

		helpers.Must(process(s))
		if gr != nil {
			gr.Flush()
		}
	}()

	return dst, q.Projections, nil
}
