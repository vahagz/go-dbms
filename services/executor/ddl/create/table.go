package create

import (
	"io"
	"path/filepath"

	"go-dbms/pkg/engine/mergetree"
	"go-dbms/pkg/pipe"
	"go-dbms/pkg/table"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/ddl/create"

	"github.com/pkg/errors"
)

func (ddl *DDLCreate) CreateTable(q *create.QueryCreateTable) (io.WriterTo, error) {
	tablePath := ddl.TablePath(q.Name)
	opts := &table.Options{
		Engine:       q.Engine,
		Columns:      q.Columns,
		DataPath:     tablePath,
		MetaFilePath: filepath.Join(tablePath, table.MetadataFileName),
	}
	var t table.ITable
	var err error
	
	switch q.Engine {
		case table.InnoDB:    t, err = table.Open(opts)
		case table.MergeTree: t, err = mergetree.Open(opts)
		default:              panic(parent.ErrInvalidEngine)
	}
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
