package ddl

import (
	"go-dbms/services/executor/ddl/create"
	"go-dbms/services/executor/parent"
	pcreate "go-dbms/services/parser/query/ddl/create"
	"io"
)

type DDL struct {
	create *create.DDLCreate
}

func New(es *parent.ExecutorService) *DDL {
	return &DDL{create: create.New(es)}
}

func (ddl *DDL) Create(q pcreate.Creater) (io.WriterTo, error) {
	return ddl.create.Create(q)
}
