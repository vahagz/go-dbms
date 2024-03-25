package create

import (
	"fmt"

	"go-dbms/services/parser/query/ddl/create"
)

func (ddl *DDLCreate) ddlCreateValidate(q create.Creater) error {
	switch q.GetTarget() {
		case create.TABLE:    return ddl.ddlCreateTableValidate(q.(*create.QueryCreateTable))
		case create.INDEX:    return ddl.ddlCreateIndexValidate(q.(*create.QueryCreateIndex))
		default:              panic(fmt.Errorf("invalid create target: '%s'", q.GetTarget()))
	}
}
