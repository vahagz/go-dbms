package create

import (
	"fmt"

	"go-dbms/services/parser/query/ddl/create"
)

func (ddl *DDLCreate) ddlCreateTableValidate(q *create.QueryCreateTable) error {
	if _, ok := ddl.Tables[q.Name]; ok {
		return fmt.Errorf("table already exists")
	}
	return nil
}
