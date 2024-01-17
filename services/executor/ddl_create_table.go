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

	for indexName, indexOptions := range q.Indexes {
		in := indexName
		if err = t.CreateIndex(&in, indexOptions.IndexOptions); err != nil {
			return nil, errors.Wrapf(err, "failed to create index: '%s'", in)
		}
	}

	p := newPipe(&EOS)
	return p, nil
}
