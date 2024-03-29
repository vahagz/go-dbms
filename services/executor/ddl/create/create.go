package create

import (
	"fmt"

	"go-dbms/pkg/types"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/ddl/create"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

type DDLCreate struct {
	*parent.ExecutorService
}

func New(es *parent.ExecutorService) *DDLCreate {
	return &DDLCreate{ExecutorService: es}
}

func (ddl *DDLCreate) Create(q create.Creater) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	if err := ddl.ddlCreateValidate(q); err != nil {
		return nil, nil, errors.Wrapf(err, "validation error")
	}

	switch q.GetTarget() {
		case create.TABLE: return ddl.CreateTable(q.(*create.QueryCreateTable))
		case create.INDEX: return ddl.CreateIndex(q.(*create.QueryCreateIndex))
		default:           panic(fmt.Errorf("invalid create target: '%s'", q.GetTarget()))
	}
}
