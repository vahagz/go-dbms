package executor

import (
	"encoding/json"
	"fmt"
	"io"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (es *ExecutorServiceT) dmlDeleteValidate(q *dml.QueryDelete) error {
	table, ok := es.tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	if q.WhereIndex != nil {
		if !table.HasIndex(q.WhereIndex.Name) {
			return fmt.Errorf("index not found: '%s'", q.WhereIndex.Name)
		}

		fs := q.WhereIndex.FilterStart
		if fs != nil {
			for k, v := range fs.Value {
				casted, err := v.Cast(table.Column(k).Typ, table.Column(k).Meta)
				if err != nil {
					return errors.Wrapf(err, "failed to cast %v to %v", v.GetCode(), table.Column(k).Typ)
				}

				fs.Value[k] = casted
			}
		}
		
		fe := q.WhereIndex.FilterEnd
		if fe != nil {
			for k, v := range fe.Value {
				casted, err := v.Cast(table.Column(k).Typ, table.Column(k).Meta)
				if err != nil {
					return errors.Wrapf(err, "failed to cast %v to %v", v.GetCode(), table.Column(k).Typ)
				}

				fe.Value[k] = casted
			}
		}

		var validateWhere func(w *statement.WhereStatement)
		validateWhere = func (w *statement.WhereStatement) {
			if w == nil {
				return
			}

			if w.And != nil {
				for _, ws := range w.And {
					validateWhere(ws)
				}
			}
			if w.Or != nil {
				for _, ws := range w.Or {
					validateWhere(ws)
				}
			}
			if w.Statement != nil {
				w.Statement.Val = types.Type(table.Column(w.Statement.Col).Meta).Set(w.Statement.Val.Value())
			}
		}
		validateWhere((*statement.WhereStatement)(q.Where))
	}

	return nil
}

func (es *ExecutorServiceT) dmlDelete(q *dml.QueryDelete) (io.Reader, error) {
	if err := es.dmlDeleteValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	t := es.tables[q.Table]
	p := newPipe(nil)

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

		columns := t.PrimaryColumns()
		process := func(row map[string]types.DataType) error {
			record := make([]interface{}, 0, len(columns))
			for _, col := range columns {
				record = append(record, row[col.Name].Value())
			}

			blob, err := json.Marshal(record)
			if err != nil {
				return errors.Wrap(err, "failed to marshal record")
			}

			_, err = p.Write(blob)
			return errors.Wrap(err, "failed to push marshaled record")
		}

		var err error
		if indexFilterStart != nil {
			err = t.DeleteByIndex(name, indexFilterStart, indexFilterEnd, filter, process)
		} else {
			err = t.Delete(filter, process)
		}

		if err != nil {
			panic(err)
		// } else if err := p.bw.Flush(); err != nil {
		// 	panic(err)
		} else if _, err := p.Write(EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
