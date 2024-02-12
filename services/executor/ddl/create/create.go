package create

import (
	"fmt"
	"io"

	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/ddl/create"

	"github.com/pkg/errors"
)

type DDLCreate struct {
	*parent.ExecutorService
}

func New(es *parent.ExecutorService) *DDLCreate {
	return &DDLCreate{ExecutorService: es}
}

func (ddl *DDLCreate) Create(q create.Creater) (io.WriterTo, error) {
	if err := ddl.ddlCreateValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	switch q.GetTarget() {
		case create.DATABASE: return ddl.CreateDatabase(q.(*create.QueryCreateDatabase))
		case create.TABLE:    return ddl.CreateTable(q.(*create.QueryCreateTable))
		case create.INDEX:    return ddl.CreateIndex(q.(*create.QueryCreateIndex))
		default:              panic(fmt.Errorf("invalid create target: '%s'", q.GetTarget()))
	}
}
