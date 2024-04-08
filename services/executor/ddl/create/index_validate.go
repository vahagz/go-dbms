package create

import (
	"fmt"

	"go-dbms/services/parser/query/ddl/create"
)

func (ddl *DDLCreate) ddlCreateIndexValidate(q *create.QueryCreateIndex) error {
	t, ok := ddl.Tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	if t.HasIndex(q.Name) {
		return fmt.Errorf("index already exists: '%s'", q.Name)
	}

	return nil
}
