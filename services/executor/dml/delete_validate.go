package dml

import (
	"fmt"

	"go-dbms/services/parser/query/dml"
)

func (dml *DML) dmlDeleteValidate(q *dml.QueryDelete) error {
	table, ok := dml.Tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	dml.validateWhereIndex(table, q.WhereIndex)
	dml.validateWhere(q.Where)

	return nil
}
