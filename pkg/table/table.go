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
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"

	"github.com/vahagz/bptree"
)

const (
	MetadataFileName = "metadata.json"
	dataFileName     = "data"
	indexPath        = "./indexes"
)
type ITable interface {
	Insert(values map[string]types.DataType) (map[string]types.DataType, error)

	Find(filter *statement.WhereStatement) []index.Entry
	FullScan(scanFn func(row map[string]types.DataType) (bool, error)) error
	FindByIndex(name string, start *index.Filter, end *index.Filter, filter *statement.WhereStatement) ([]map[string]types.DataType, error)
	FullScanByIndex(indexName string, reverse bool, scanFn func(row map[string]types.DataType) (bool, error)) error
	ScanByIndex(name string, start *index.Filter, end *index.Filter, scanFn func(row map[string]types.DataType) (bool, error)) error

	Update(filter *statement.WhereStatement, updateValuesMap map[string]types.DataType, scanFn func(row map[string]types.DataType) error) error
	UpdateByIndex(name string, start *index.Filter, end *index.Filter, filter *statement.WhereStatement, updateValuesMap map[string]types.DataType, scanFn func(row map[string]types.DataType) error) error

	Delete(filter *statement.WhereStatement, scanFn func(row map[string]types.DataType) error) error
	DeleteByIndex(name string, start *index.Filter, end *index.Filter, filter *statement.WhereStatement, scanFn func(row map[string]types.DataType) error) error

	PrepareSpace(rows int)

	Column(name string) *column.Column
	Columns() []*column.Column
	ColumnsMap() map[string]*column.Column

	CreateIndex(name *string, opts *index.IndexOptions) error
	HasIndex(name string) bool

	PrimaryColumns() []*column.Column
	PrimaryKey() string

	Engine() Engine
	Close() error
}

type Table struct {
	dataPath, metaFilePath string

	mu, metaMu *sync.RWMutex
	df         *data.DataFile
	meta       *Metadata
	indexes    map[string]*index.Index
}

func Open(opts *Options) (ITable, error) {
	table := &Table{
		mu:           &sync.RWMutex{},
		metaMu:       &sync.RWMutex{},
		dataPath:     opts.DataPath,
		metaFilePath: opts.MetaFilePath,
		indexes:      map[string]*index.Index{},
		df:           &data.DataFile{},
	}

	err := table.init(opts)
	if err != nil {
		return nil, err
	}

	dataOptions := data.DefaultOptions
	dataOptions.Columns = table.meta.Columns

	df, err := data.Open(
		path.Join(table.dataPath, dataFileName),
		&dataOptions,
	)
	if err != nil {
		return nil, err
	}

	*table.df = *df
	return table, nil
}

func (t *Table) HasIndex(name string) bool {
	_, ok := t.indexes[name]
	return ok
}

func (t *Table) Columns() []*column.Column {
	return t.meta.Columns
}

func (t *Table) PrimaryKey() string {
	return t.meta.PrimaryKey
}

func (t *Table) PrimaryColumns() []*column.Column {
	return t.indexes[t.meta.PrimaryKey].Columns()
}

func (t *Table) ColumnsMap() map[string]*column.Column {
	return t.meta.ColumnsMap
}

func (t *Table) Column(name string) *column.Column {
	return t.meta.ColumnsMap[name]
}

func (t *Table) PrepareSpace(rows int) {
	t.df.PrepareSpace(uint32(rows * int(t.df.HeapSize() / t.df.Count())))
	for _, i := range t.indexes {
		i.PrepareSpace(rows)
	}
}

func (t *Table) Engine() Engine {
	return t.meta.Engine
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

func (t *Table) indexPath(name string) string {
	return path.Join(t.dataPath, indexPath, name)
}

func (t *Table) readMeta(opts *Options) error {
	defer func ()  {
		for _, c := range t.meta.Columns {
			t.meta.ColumnsMap[c.Name] = c
		}
	}()

	if opts.Meta != nil {
		t.meta = opts.Meta
	} else if _, err := os.Stat(t.metaFilePath); os.IsNotExist(err) {
		t.meta = &Metadata{
			Engine:     opts.Engine,
			Indexes:    []*index.Meta{},
			Columns:    opts.Columns,
			ColumnsMap: map[string]*column.Column{},
		}

		return t.writeMeta()
  }

	metadataBytes, err := os.ReadFile(t.metaFilePath)
	if err != nil {
		return err
	}

	t.meta = &Metadata{
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
		if k != t.meta.PrimaryKey {
			i.SetPK(t.indexes[t.meta.PrimaryKey])
		}
	}

	return nil
}

func (t *Table) writeMeta() error {
	if t.metaFilePath == "" {
		return nil
	}

	metadataBytes, _ := json.Marshal(t.meta)
	return os.WriteFile(t.metaFilePath, metadataBytes, 0644)
}

func (t *Table) createDirs() error {
	if err := helpers.CreateDir(t.dataPath); err != nil {
		return err
	}
	return helpers.CreateDir(path.Join(t.dataPath, indexPath))
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
	pkCols := t.indexes[t.meta.PrimaryKey].Columns()
	pkRow := make(map[string]types.DataType, 1)
	for _, col := range pkCols {
		pkRow[col.Name] = row[col.Name]
	}
	return pkRow
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
	t.metaMu.Lock()
	defer t.metaMu.Unlock()

	for _, column := range t.meta.Columns {
		if _, ok := row[column.Name]; !ok {
			row[column.Name] = column.Meta.Default()
		}
	}
}

func (t *Table) isPK(i *index.Index) bool {
	return t.meta.PrimaryKey == i.Meta().Name
}
