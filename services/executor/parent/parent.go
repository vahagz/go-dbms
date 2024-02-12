package parent

import (
	"go-dbms/pkg/table"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type ExecutorService struct {
	dataPath string
	Tables   map[string]*table.Table
}

func New(dataPath string) (*ExecutorService, error) {
	dirEntries, err := os.ReadDir(dataPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read tables directory")
	}

	es := &ExecutorService{
		dataPath: dataPath,
		Tables:   make(map[string]*table.Table, len(dirEntries)),
	}

	for _, de := range dirEntries {
		if de.IsDir() {
			es.Tables[de.Name()], err = table.Open(es.TablePath(de.Name()), nil)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to open table: '%s'", de.Name())
			}
		}
	}

	return es, nil
}

func (es *ExecutorService) Close() error {
	for name, table := range es.Tables {
		if err := table.Close(); err != nil {
			return errors.Wrapf(err, "failed to close table: '%s'", name)
		}
	}
	return nil
}

func (es *ExecutorService) TablePath(tableName string) string {
	return filepath.Join(es.dataPath, tableName)
}
