package create

import (
	"io"

	"go-dbms/services/parser/query/ddl/create"
)

func (ddl *DDLCreate) CreateDatabase(q *create.QueryCreateDatabase) (io.WriterTo, error) {
	return nil, nil
}
