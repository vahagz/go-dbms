package create

import (
	"go-dbms/pkg/types"
	"go-dbms/services/parser/query/ddl/create"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/stream"
)

func (ddl *DDLCreate) CreateIndex(q *create.QueryCreateIndex) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	return nil, nil, ddl.Tables[q.Table].CreateIndex(&q.Name, q.IndexOptions)
}
