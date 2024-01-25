package executor

import (
	"io"

	"go-dbms/pkg/table"
	"go-dbms/services/parser/query/ddl/create"

	"github.com/pkg/errors"
)

func (es *ExecutorServiceT) ddlCreateTableValidate(q *create.QueryCreateTable) error {
	if _, ok := es.tables[q.Name]; ok {
		return errors.New("table already exists")
	}
	return nil
}

func (es *ExecutorServiceT) ddlCreateTable(q *create.QueryCreateTable) (io.Reader, error) {
	t, err := table.Open(es.tablePath(q.Name), &table.Options{
		Columns: q.Columns,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create table: '%s'", q.Name)
	}

	for _, idx := range q.Indexes {
		if err = t.CreateIndex(&idx.Name, idx.IndexOptions); err != nil {
			return nil, errors.Wrapf(err, "failed to create index: '%s'", idx.Name)
		}
	}

	es.tables[q.Name] = t

	return newPipe(EOS), nil
}
