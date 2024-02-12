package dml

import (
	"fmt"

	"go-dbms/services/parser/query/dml"
)

func (dml *DML) dmlPrepareValidate(q *dml.QueryPrepare) error {
	_, ok := dml.Tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	if q.Rows < 0 {
		return fmt.Errorf("rows must be positive integer")
	}

	return nil
}
