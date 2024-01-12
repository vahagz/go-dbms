package table

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"

	"go-dbms/pkg/column"
	"go-dbms/pkg/data"
	"go-dbms/pkg/index"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"

	"github.com/vahagz/bptree"
)

const (
	metadataFileName = "metadata.json"
	dataFileName     = "data"
	indexPath        = "./indexes"
)

type Table struct {
	path    string
	mu      *sync.RWMutex
	df      *data.DataFile
	meta    *metadata
	indexes map[string]*index.Index
}

func Open(tablePath string, opts *Options) (*Table, error) {
	dataOptions := data.DefaultOptions
	dataOptions.Columns = opts.Columns

	df, err := data.Open(
		path.Join(tablePath, dataFileName),
		&dataOptions,
	)
	if err != nil {
		return nil, err
	}

	table := &Table{
		mu:      &sync.RWMutex{},
		path:    tablePath,
		df:      df,
		indexes: map[string]*index.Index{},
	}

	return table, table.init(opts)
}

func (t *Table) Columns() []*column.Column {
	return t.meta.Columns
}

func (t *Table) PrimaryColumns() []*column.Column {
	if t.meta.PrimaryKey == nil {
		return nil
	}
	return t.indexes[*t.meta.PrimaryKey].Columns()
}

func (t *Table) ColumnsMap() map[string]*column.Column {
	return t.meta.ColumnsMap
}

func (t* Table) Column(name string) *column.Column {
	return t.meta.ColumnsMap[name]
}

func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	err := t.writeMeta()
	if err != nil {
		return err
	}

	for _, index := range t.indexes {
		index.Close()
	}

	return t.df.Close()
}

func (t *Table) init(opts *Options) error {
	err := t.createDirs()
	if err != nil {
		return err
	}

	err = t.readMeta(opts)
	if err != nil {
		return err
	}

	return t.readIndexes()
}

func (t *Table) metaPath() string {
	return path.Join(t.path, metadataFileName)
}

func (t *Table) indexPath(name string) string {
	return path.Join(t.path, indexPath, name)
}

func (t *Table) readMeta(opts *Options) error {
	defer func ()  {
		for _, c := range t.meta.Columns {
			t.meta.ColumnsMap[c.Name] = c
		}
	}()

	metaPath := t.metaPath()

  if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.meta = &metadata{
			Indexes:    []*index.Meta{},
			PrimaryKey: nil,
			Columns:    opts.Columns,
			ColumnsMap: map[string]*column.Column{},
		}

		return t.writeMeta()
  }

	metadataBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}

	t.meta = &metadata{
		ColumnsMap: map[string]*column.Column{},
	}

	return json.Unmarshal(metadataBytes, t.meta)
}

func (t *Table) readIndexes() error {
	for _, metaindex := range t.meta.Indexes {	
		bpt, err := bptree.Open(
			t.indexPath(metaindex.Name),
			metaindex.Options,
		)
		if err != nil {
			return err
		}

		columns := make([]*column.Column, 0, len(metaindex.Columns))
		for _, colName := range metaindex.Columns {
			columns = append(columns, t.meta.ColumnsMap[colName])
		}

		t.indexes[metaindex.Name] = index.New(
			metaindex,
			t.df,
			bpt,
			columns,
			metaindex.Uniq,
		)
	}

	for k, i := range t.indexes {
		if k != *t.meta.PrimaryKey {
			i.SetPK(t.indexes[*t.meta.PrimaryKey])
		}
	}

	return nil
}

func (t *Table) writeMeta() error {
	metadataBytes, _ := json.Marshal(t.meta)
	return os.WriteFile(t.metaPath(), metadataBytes, 0644)
}

func (t *Table) createDirs() error {
	if err := helpers.CreateDir(t.path); err != nil {
		return err
	}
	return helpers.CreateDir(path.Join(t.path, indexPath))
}

func (t *Table) map2row(rowMap map[string]types.DataType) []types.DataType {
	row := make([]types.DataType, 0, len(rowMap))
	for _, c := range t.meta.Columns {
		if val, ok := rowMap[c.Name]; ok {
			row = append(row, val)
		}
	}
	return row
}

func (t *Table) row2map(row []types.DataType) map[string]types.DataType {
	rowMap := map[string]types.DataType{}
	for i, data := range row {
		rowMap[t.meta.Columns[i].Name] = data
	}
	return rowMap
}

func (t *Table) row2pk(row map[string]types.DataType) map[string]types.DataType {
	if t.meta.PrimaryKey == nil {
		return nil
	}

	pkCols := t.indexes[*t.meta.PrimaryKey].Columns()
	pkRow := make(map[string]types.DataType, 1)
	for _, col := range pkCols {
		pkRow[col.Name] = row[col.Name]
	}
	return pkRow
}

func (t *Table) rows2pk(rows []map[string]types.DataType) []map[string]types.DataType {
	pkRows := make([]map[string]types.DataType, 0, len(rows))
	for _, row := range rows {
		pkRows = append(pkRows, t.row2pk(row))
	}
	return pkRows
}

func (t *Table) validateMap(row map[string]types.DataType) error {
	if len(row) > len(t.meta.Columns) {
		return fmt.Errorf("invalid columns count")
	}

	for columnName := range row {
		if _, ok := t.meta.ColumnsMap[columnName]; !ok {
			return fmt.Errorf("unknown column while inserting => %s", columnName)
		}
	}
	return nil
}

func (t *Table) setDefaults(row map[string]types.DataType) {
	for _, column := range t.meta.Columns {
		if _, ok := row[column.Name]; !ok {
			row[column.Name] = column.Meta.Default()
		}
	}
}

func (t *Table) isPK(i *index.Index) bool {
	return t.meta.PrimaryKey != nil && *t.meta.PrimaryKey == i.Meta().Name
}
