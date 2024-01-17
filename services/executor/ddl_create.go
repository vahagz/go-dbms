package executor

import (
	"fmt"
	"io"

	"go-dbms/services/parser/query/ddl/create"

	"github.com/pkg/errors"
)

func (es *ExecutorServiceT) ddlCreateValidate(q create.Creater) error {
	switch q.GetTarget() {
		case create.DATABASE: return es.ddlCreateDatabaseValidate(q.(*create.QueryCreateDatabase))
		case create.TABLE:    return es.ddlCreateTableValidate(q.(*create.QueryCreateTable))
		case create.INDEX:    return es.ddlCreateIndexValidate(q.(*create.QueryCreateIndex))
		default:              panic(fmt.Errorf("invalid create target: '%s'", q.GetTarget()))
	}
}

func (es *ExecutorServiceT) ddlCreate(q create.Creater) (io.Reader, error) {
	if err := es.ddlCreateValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	switch q.GetTarget() {
		case create.DATABASE: return es.ddlCreateDatabase(q.(*create.QueryCreateDatabase))
		case create.TABLE:    return es.ddlCreateTable(q.(*create.QueryCreateTable))
		case create.INDEX:    return es.ddlCreateIndex(q.(*create.QueryCreateIndex))
		default:              panic(fmt.Errorf("invalid create target: '%s'", q.GetTarget()))
	}
}
