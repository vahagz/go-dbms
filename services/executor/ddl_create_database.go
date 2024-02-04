package executor

import (
	"io"

	"go-dbms/services/parser/query/ddl/create"
)

func (es *ExecutorServiceT) ddlCreateDatabaseValidate(q *create.QueryCreateDatabase) error {
	return nil
}

func (es *ExecutorServiceT) ddlCreateDatabase(q *create.QueryCreateDatabase) (io.WriterTo, error) {
	return nil, nil
}
