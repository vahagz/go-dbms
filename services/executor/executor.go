package executor

import (
	"fmt"
	"io"
	"os"
	"path"

	"go-dbms/pkg/table"
	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/ddl/create"
	"go-dbms/services/parser/query/dml"

	"github.com/pkg/errors"
)

type ExecutorService interface {
	Exec(q query.Querier) (io.Reader, error)
}

type ExecutorServiceT struct {
	dataPath string
	tables   map[string]*table.Table
}

func New(dataPath string) (*ExecutorServiceT, error) {
	dirEntries, err := os.ReadDir(dataPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read tables directory")
	}

	es := &ExecutorServiceT{
		dataPath: dataPath,
		tables:   make(map[string]*table.Table, len(dirEntries)),
	}

	for _, de := range dirEntries {
		if de.IsDir() {
			es.tables[de.Name()], err = table.Open(es.tablePath(de.Name()), nil)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to open table: '%s'", de.Name())
			}
		}
	}

	return es, nil
}

func (es *ExecutorServiceT) Exec(q query.Querier) (io.WriterTo, error) {
	switch q.GetType() {
		case query.CREATE:  return es.ddlCreate(q.(create.Creater))
		case query.DELETE:  return es.dmlDelete(q.(*dml.QueryDelete))
		case query.INSERT:  return es.dmlInsert(q.(*dml.QueryInsert))
		case query.SELECT:  return es.dmlSelect(q.(*dml.QuerySelect))
		case query.UPDATE:  return es.dmlUpdate(q.(*dml.QueryUpdate))
		case query.PREPARE: return es.dmlPrepare(q.(*dml.QueryPrepare))
		default:            panic(fmt.Errorf("invalid query type: '%s'", q.GetType()))
	}
}

func (es *ExecutorServiceT) Close() error {
	for name, table := range es.tables {
		if err := table.Close(); err != nil {
			return errors.Wrapf(err, "failed to close table: '%s'", name)
		}
	}
	return nil
}

func (es *ExecutorServiceT) tablePath(tableName string) string {
	return path.Join(es.dataPath, tableName)
}
