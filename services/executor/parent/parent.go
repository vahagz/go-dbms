package parent

import (
	"encoding/json"
	"os"
	"path/filepath"

	"go-dbms/pkg/engine/mergetree"
	"go-dbms/pkg/table"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

const enginesFile = "engines.json"

var ErrInvalidEngine = errors.New("invalid engine")

type ExecutorService struct {
	dataPath string
	Tables   map[string]table.ITable
}

func New(dataPath string) (*ExecutorService, error) {
	dirEntries, err := os.ReadDir(dataPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read tables directory")
	}

	enginesMap := map[string]table.Engine{}
	enginesFilePath := filepath.Join(dataPath, enginesFile)
	if _, err := os.Stat(enginesFilePath); err != nil && !os.IsNotExist(err) {
		panic(err)
  } else if err == nil {
		if err := json.Unmarshal(helpers.MustVal(os.ReadFile(enginesFilePath)), &enginesMap); err != nil {
			panic(err)
		}
	}

	es := &ExecutorService{
		dataPath: dataPath,
		Tables:   make(map[string]table.ITable, len(dirEntries)),
	}

	for _, de := range dirEntries {
		if !de.IsDir() {
			continue
		}

		tableName := de.Name()
		dataPath := es.TablePath(tableName)
		metaFilePath := filepath.Join(dataPath, table.MetadataFileName)
		engine := enginesMap[tableName]
		opts := &table.Options{
			Engine:       engine,
			DataPath:     dataPath,
			MetaFilePath: metaFilePath,
		}

		switch engine {
			case table.InnoDB:    es.Tables[tableName], err = table.Open(opts)
			case table.MergeTree: es.Tables[tableName], err = mergetree.Open(opts)
			default:              panic(ErrInvalidEngine)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "failed to open table: '%s'", tableName)
		}
	}

	return es, nil
}

func (es *ExecutorService) Close() error {
	enginesMap := map[string]table.Engine{}
	for name, table := range es.Tables {
		enginesMap[name] = table.Engine()
		table.Close()
	}

	return os.WriteFile(
		filepath.Join(es.dataPath, enginesFile),
		helpers.MustVal(json.Marshal(enginesMap)),
		0644,
	)
}

func (es *ExecutorService) TablePath(tableName string) string {
	return filepath.Join(es.dataPath, tableName)
}
