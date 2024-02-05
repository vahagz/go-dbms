package executor

import (
	"fmt"
	"io"

	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

func (es *ExecutorServiceT) dmlPrepareValidate(q *dml.QueryPrepare) error {
	_, ok := es.tables[q.Table]
	if !ok {
		return fmt.Errorf("table not found: '%s'", q.Table)
	}

	if q.Rows < 0 {
		return fmt.Errorf("rows must be positive integer")
	}

	return nil
}

func (es *ExecutorServiceT) dmlPrepare(q *dml.QueryPrepare) (io.WriterTo, error) {
	if err := es.dmlPrepareValidate(q); err != nil {
		return nil, errors.Wrapf(err, "validation error")
	}

	p := newPipe(EOS)
	go func() {
		es.tables[q.Table].PrepareSpace(q.Rows)
		if _, err := p.Write(EOS); err != nil {
			panic(err)
		}
	}()

	return p, nil
}
