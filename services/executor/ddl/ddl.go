package ddl

import (
	"go-dbms/pkg/types"
	"go-dbms/services/executor/ddl/create"
	"go-dbms/services/executor/parent"
	pcreate "go-dbms/services/parser/query/ddl/create"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/stream"
)

type DDL struct {
	create *create.DDLCreate
}

func New(es *parent.ExecutorService) *DDL {
	return &DDL{create: create.New(es)}
}

func (ddl *DDL) Create(q pcreate.Creater, es parent.Executor) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	return ddl.create.Create(q)
}
