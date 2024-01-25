package executor

import (
	"fmt"
	"io"

	"go-dbms/services/parser/query/ddl/create"

	"github.com/pkg/errors"
)

func (es *ExecutorServiceT) ddlCreateIndexValidate(q *create.QueryCreateIndex) error {
	t, ok := es.tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	if t.HasIndex(q.Name) {
		return fmt.Errorf("index already exists: '%s'", q.Name)
	}

	return nil
}

func (es *ExecutorServiceT) ddlCreateIndex(q *create.QueryCreateIndex) (io.Reader, error) {
	err := es.tables[q.Table].CreateIndex(&q.Name, q.IndexOptions)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create index: '%s'", q.Name)
	}
	return newPipe(EOS), nil
}
