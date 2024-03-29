package dml

import (
	"go-dbms/pkg/types"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query/dml"
	"go-dbms/services/parser/query/dml/projection"
	"go-dbms/util/stream"

	"github.com/pkg/errors"
)

func (dml *DML) Prepare(q *dml.QueryPrepare, es parent.Executor) (
	stream.ReaderContinue[types.DataRow],
	*projection.Projections,
	error,
) {
	if err := dml.dmlPrepareValidate(q); err != nil {
		return nil, nil, errors.Wrapf(err, "validation error")
	}

	dml.Tables[q.Table].PrepareSpace(q.Rows)

	return nil, nil, nil
}
