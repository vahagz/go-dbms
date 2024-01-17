package executor

import (
	"fmt"
	"io"
	"os"
	"path"

	"go-dbms/pkg/table"
	"go-dbms/services/parser/query"
	"go-dbms/services/parser/query/ddl/create"

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

func (es *ExecutorServiceT) Exec(q query.Querier) (io.Reader, error) {
	switch q.GetType() {
		case query.CREATE: return es.ddlCreate(q.(create.Creater))
		// case query.DELETE: return es.ddlCreate(q.(*create.QueryCreate))
		// case query.INSERT: return es.ddlCreate(q.(*create.QueryCreate))
		// case query.SELECT: return es.ddlCreate(q.(*create.QueryCreate))
		// case query.UPDATE: return es.ddlCreate(q.(*create.QueryCreate))
		default:           panic(fmt.Errorf("invalid query type: '%s'", q.GetType()))
	}
}

func (es *ExecutorServiceT) tablePath(tableName string) string {
	return path.Join(es.dataPath, tableName)
}
