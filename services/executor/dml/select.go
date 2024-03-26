package dml

import (
	"cmp"
	"encoding/json"
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/eval"
	"go-dbms/services/parser/query/dml/group"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (dml *DML) Select(q *dml.QuerySelect) (io.WriterTo, error) {
	if err := dml.dmlSelectValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := dml.Tables[q.Table]
	p := pipe.NewPipe(nil)

	var gr *group.Group
	if len(q.Projections.Aggregators()) != 0 {
		gr = group.New(q.Projections, q.GroupBy, p)
	}

	go func() {
		nonAggr := q.Projections.NonAggregators()
		prList := q.Projections.Iterator()
		record := make([]interface{}, len(prList))

		process := func(s stream.ReaderContinue[types.DataRow]) error {
			for row, ok := s.Pop(); ok; row, ok = s.Pop() {
				s.Continue(true)
				if gr == nil {
					for i, p := range prList {
						val := eval.Eval(row, p)
						row[p.Alias] = val
						record[i] = val.Value()
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

				if _, err := p.Write(helpers.MustVal(json.Marshal(record))); err != nil {			
					return errors.Wrap(err, "failed to push marshaled record")
				}
			}
			return nil
		}

		var s stream.ReaderContinue[types.DataRow]
		if q.WhereIndex != nil {
			s = helpers.MustVal(t.ScanByIndex(
				q.UseIndex,
				q.WhereIndex.FilterStart,
				q.WhereIndex.FilterEnd,
			))
		} else {
			s = helpers.MustVal(t.FullScanByIndex(cmp.Or(q.UseIndex, t.PrimaryKey()), false))
		}

		helpers.Must(process(s))
		if gr != nil {
			gr.Flush()
		}
		helpers.MustVal(p.Write(pipe.EOS))
	}()

	return p, nil
}
