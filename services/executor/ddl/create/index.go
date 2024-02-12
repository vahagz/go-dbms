package create

import (
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/services/parser/query/ddl/create"

	"github.com/pkg/errors"
)

func (ddl *DDLCreate) CreateIndex(q *create.QueryCreateIndex) (io.WriterTo, error) {
	err := ddl.Tables[q.Table].CreateIndex(&q.Name, q.IndexOptions)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create index: '%s'", q.Name)
	}
	return pipe.NewPipe(pipe.EOS), nil
}
