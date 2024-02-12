package executor

import (
	"fmt"
	"io"

	"go-dbms/services/executor/ddl"
	"go-dbms/services/executor/dml"
	"go-dbms/services/executor/parent"
	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/ddl/create"
	pdml "go-dbms/services/parser/query/dml"
)

type ExecutorService struct {
	es  *parent.ExecutorService
	dml *dml.DML
	ddl *ddl.DDL
}

func New(dataPath string) (*ExecutorService, error) {
	es, err := parent.New(dataPath)
	if err != nil {
		return nil, err
	}

	return &ExecutorService{
		es:  es,
		dml: dml.New(es),
		ddl: ddl.New(es),
	}, nil
}

func (es *ExecutorService) Exec(q query.Querier) (io.WriterTo, error) {
	switch q.GetType() {
		case query.CREATE:  return es.ddl.Create(q.(create.Creater))
		case query.DELETE:  return es.dml.Delete(q.(*pdml.QueryDelete))
		case query.INSERT:  return es.dml.Insert(q.(*pdml.QueryInsert))
		case query.SELECT:  return es.dml.Select(q.(*pdml.QuerySelect))
		case query.UPDATE:  return es.dml.Update(q.(*pdml.QueryUpdate))
		case query.PREPARE: return es.dml.Prepare(q.(*pdml.QueryPrepare))
		default:            panic(fmt.Errorf("invalid query type: '%s'", q.GetType()))
	}
}

func (es *ExecutorService) Close() error {
	return es.es.Close()
}
