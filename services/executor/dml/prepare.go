package dml

import (
	"io"

	"go-dbms/pkg/pipe"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (dml *DML) Prepare(q *dml.QueryPrepare) (io.WriterTo, error) {
	if err := dml.dmlPrepareValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	p := pipe.NewPipe(pipe.EOS)
	go func() {
		dml.Tables[q.Table].PrepareSpace(q.Rows)
		if _, err := p.Write(pipe.EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
