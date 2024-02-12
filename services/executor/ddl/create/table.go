package create

import (
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/pkg/table"
	"go-dbms/services/parser/query/ddl/create"

	"github.com/pkg/errors"
)

func (ddl *DDLCreate) CreateTable(q *create.QueryCreateTable) (io.WriterTo, error) {
	t, err := table.Open(ddl.TablePath(q.Name), &table.Options{
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

	ddl.Tables[q.Name] = t

	return pipe.NewPipe(pipe.EOS), nil
}
