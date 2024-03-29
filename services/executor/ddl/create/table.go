package create

import (
	"path/filepath"

	"go-dbms/pkg/engine/aggregatingmergetree"
	"go-dbms/pkg/engine/mergetree"
	"go-dbms/pkg/table"
	"go-dbms/pkg/types"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/ddl/create"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (ddl *DDLCreate) CreateTable(q *create.QueryCreateTable) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
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
		case table.InnoDB:               t, err = table.Open(opts)
		case table.MergeTree:            t, err = mergetree.Open(opts)
		case table.AggregatingMergeTree: t, err = aggregatingmergetree.Open(&aggregatingmergetree.Options{
			Options:      opts,
			Aggregations: q.AggrFunc,
		})
		default:              panic(parent.ErrInvalidEngine)
	}
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create table: '%s'", q.Name)
	}

	for _, idx := range q.Indexes {
		if err = t.CreateIndex(&idx.Name, idx.IndexOptions); err != nil {
			return nil, nil, errors.Wrapf(err, "failed to create index: '%s'", idx.Name)
		}
	}

	ddl.Tables[q.Name] = t

	return nil, nil, nil
}
