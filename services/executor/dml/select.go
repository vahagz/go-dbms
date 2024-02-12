package dml

import (
	"encoding/json"
	"io"

	"go-dbms/pkg/index"
	"go-dbms/pkg/pipe"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/function"
	"go-dbms/services/parser/query/dml/group"
	"go-dbms/services/parser/query/dml/projection"

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
		var (
			name string
			indexFilterStart, indexFilterEnd *index.Filter
			filter *statement.WhereStatement
		)

		if q.WhereIndex != nil {
			name = q.WhereIndex.Name
			if q.WhereIndex.FilterStart != nil {
				indexFilterStart = &index.Filter{
					Operator: q.WhereIndex.FilterStart.Operator,
					Value:    q.WhereIndex.FilterStart.Value,
				}

				if q.WhereIndex.FilterEnd != nil {
					indexFilterEnd = &index.Filter{
						Operator: q.WhereIndex.FilterEnd.Operator,
						Value:    q.WhereIndex.FilterEnd.Value,
					}
				}
			}
		}
		if q.Where != nil {
			filter = (*statement.WhereStatement)(q.Where)
		}

		prList := q.Projections.Iterator()
		record := make([]interface{}, len(prList))

		var applyArgs func(row map[string]types.DataType, p *projection.Projection)
		applyArgs = func(row map[string]types.DataType, p *projection.Projection) {
			switch p.Type {
				case projection.LITERAL:
					row[p.Alias] = p.Literal
				case projection.IDENTIFIER:
					row[p.Alias] = row[p.Name]
				case projection.FUNCTION, projection.AGGREGATOR:
					for _, arg := range p.Arguments {
						applyArgs(row, arg)
					}
					if p.Type == projection.FUNCTION {
						row[p.Alias] = function.New(function.FunctionType(p.Name), p.Arguments).Apply(row)
					}
			}
		}

		process := func(row map[string]types.DataType) (bool, error) {
			for _, p := range q.Projections.Iterator() {
				applyArgs(row, p)
			}

			if gr != nil {
				gr.Add(row)
				return false, nil
			}

			clear(record)
			for i, pr := range prList {
				record[i] = row[pr.Name].Value()
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
		if indexFilterStart != nil {
			err = t.ScanByIndex(name, indexFilterStart, indexFilterEnd, filter, process)
		} else {
			err = t.FullScanByIndex(t.PrimaryKey(), false, filter, process)
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
