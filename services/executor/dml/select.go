package dml

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/eval"
	"go-dbms/services/parser/query/dml/group"

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

		process := func(row map[string]types.DataType) (bool, error) {
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
				return false, nil
			}

			if gr != nil {
				gr.Add(row)
				return false, nil
			}

			blob, err := json.Marshal(record)
			if err != nil {
				return true, errors.Wrap(err, "failed to marshal record")
			}

			_, err = p.Write(blob)
			if err != nil {			
				return true, errors.Wrap(err, "failed to push marshaled record")
			}
			return false, nil
		}

		var err error
		if q.WhereIndex != nil {
			err = t.ScanByIndex(
				q.WhereIndex.Name,
				q.WhereIndex.FilterStart,
				q.WhereIndex.FilterEnd,
				process,
			)
		} else {
			err = t.FullScanByIndex(t.PrimaryKey(), false, process)
		}

		if gr != nil {
			gr.Flush()
		}

		if err != nil {
			panic(err)
		} else if _, err := p.Write(pipe.EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
