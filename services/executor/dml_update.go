package executor

import (
	"io"

	"go-dbms/services/parser/query/dml"
)

func (es *ExecutorServiceT) dmlUpdateValidate(q *dml.QueryUpdate) error {
	// if _, ok := es.tables[q.Name]; ok {
	// 	return errors.New("table already exists")
	// }
	return nil
}

func (es *ExecutorServiceT) dmlUpdate(q *dml.QueryUpdate) (io.Reader, error) {
	// t, err := table.Open(es.tablePath(q.Name), &table.Options{
	// 	Columns: q.Columns,
	// })
	// if err != nil {
	// 	return nil, errors.Wrapf(err, "failed to create table: '%s'", q.Name)
	// }

	// for _, idx := range q.Indexes {
	// 	if err = t.CreateIndex(&idx.Name, idx.IndexOptions); err != nil {
	// 		return nil, errors.Wrapf(err, "failed to create index: '%s'", idx.Name)
	// 	}
	// }

	return newPipe(EOS), nil
}
