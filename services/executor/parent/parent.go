package parent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go-dbms/pkg/engine/mergetree"
	"go-dbms/pkg/table"
	"go-dbms/util/timer"

	"github.com/pkg/errors"
)

var ErrInvalidEngine = errors.New("invalid engine")

type tableMetaEngine struct {
	Engine table.Engine `json:"engine"`
}

type ExecutorService struct {
	dataPath string
	Tables   map[string]table.ITable
}

func New(dataPath string) (*ExecutorService, error) {
	dirEntries, err := os.ReadDir(dataPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read tables directory")
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

		mf, err := os.Open(metaFilePath)
		if err != nil {
			fmt.Printf("[error] => %v\n", err)
			continue
		}

		engineMeta := &tableMetaEngine{}
		err = json.NewDecoder(mf).Decode(engineMeta)
		if err != nil {
			fmt.Printf("[error] => %v\n", err)
			continue
		}

		opts := &table.Options{
			Engine:       engineMeta.Engine,
			DataPath:     dataPath,
			MetaFilePath: metaFilePath,
		}

		switch engineMeta.Engine {
			case table.InnoDB:    es.Tables[tableName], err = table.Open(opts)
			case table.MergeTree: es.Tables[tableName], err = mergetree.Open(opts)
			default:              panic(ErrInvalidEngine)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "failed to open table: '%s'", tableName)
		}
	}

	es.StartMerger()
	return es, nil
}

func (es *ExecutorService) StartMerger() {
	timer.SetInterval(time.Minute, func() {
		for _, t := range es.Tables {
			if t, ok := t.(mergetree.IMergeTree); ok {
				t.Merge()
			}
		}
	})
}

func (es *ExecutorService) Close() {
	for _, table := range es.Tables {
		table.Close()
	}
}

func (es *ExecutorService) TablePath(tableName string) string {
	return filepath.Join(es.dataPath, tableName)
}
