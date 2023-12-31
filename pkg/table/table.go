package table

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"go-dbms/pkg/column"
	"go-dbms/pkg/data"
	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
	"github.com/vahagz/bptree"
	allocator "github.com/vahagz/disk-allocator/heap"
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

type entry struct{
	ptr  allocator.Pointable
	data map[string]types.DataType
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

func (t *Table) Insert(values map[string]types.DataType) (allocator.Pointable, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	dataToInsert := []types.DataType{}
	dataToInsertMap := map[string]types.DataType{}
	
	if len(values) > len(t.meta.Columns) {
		return nil, fmt.Errorf("invalid columns count")
	}

	for columnName := range values {
		if _, ok := t.meta.ColumnsMap[columnName]; !ok {
			return nil, fmt.Errorf("unknown column while inserting => %s", columnName)
		}
	}

	for _, column := range t.meta.Columns {
		if data, ok := values[column.Name]; !ok {
			dataToInsert = append(dataToInsert, column.Meta.Default())
		} else {
			dataToInsert = append(dataToInsert, data)
		}
		dataToInsertMap[column.Name] = dataToInsert[len(dataToInsert)-1]
	}

	canInsert := true
	var conflictIndex string
	for _, index := range t.indexes {
		if !index.CanInsert(dataToInsertMap) {
			canInsert = false
			conflictIndex = index.Meta().Name
			break
		}
	}

	if !canInsert {
		return nil, fmt.Errorf("can't insert, '%s' causes conflict", conflictIndex)
	}

	ptr, err := t.df.Insert(dataToInsert)
	if err != nil {
		return nil, err
	}

	for _, index := range t.indexes {
		err = index.Insert(ptr, dataToInsertMap)
		if err != nil {
			panic(errors.Wrapf(err, "failed to insert into index:'%s'", index.Meta().Name))
		}
	}

	return ptr, nil
}

func (t *Table) FindByIndex(name string, start, end *index.Filter, filter *statement.WhereStatement) (
	[]map[string]types.DataType,
	error,
) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	index, ok := t.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	result := []map[string]types.DataType{}
	return result, index.ScanFilter(start, end, func(ptr allocator.Pointable) (stop bool, err error) {
		row := t.Get(ptr)
		if filter == nil || filter.Compare(row) {
			result = append(result, row)
		}
		return false, nil
	})
}

func (t *Table) DeleteByIndex(name string, start, end *index.Filter, filter *statement.WhereStatement) (
	[]map[string]types.DataType,
	error,
) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	index, ok := t.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	entries := []entry{}

	err :=  index.ScanFilter(start, end, func(ptr allocator.Pointable) (stop bool, err error) {
		row := t.Get(ptr)
		if filter == nil || filter.Compare(row) {
			entries = append(entries, entry{ptr, row})
		}
		return false, nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to scan index => '%s'", name)
	}

	result := make([]map[string]types.DataType, 0, len(entries))
	for _, e := range entries {
		for name, i := range t.indexes {
			withPK := t.meta.PrimaryKey != nil
			if t.meta.PrimaryKey != nil && *t.meta.PrimaryKey == name {
				withPK = false
			}
	
			_, err = i.Delete(e.data, withPK)
			if err != nil {
				return nil, errors.Wrap(err, "error while deleting from index")
			}
		}
	
		t.df.DeleteMem(e.ptr)
		result = append(result, t.row2pk(e.data))
	}

	return result, nil
}

func (t *Table) Get(ptr allocator.Pointable) map[string]types.DataType {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.row2map(t.df.Get(ptr))
}

func (t *Table) FullScan(scanFn func(ptr allocator.Pointable, row map[string]types.DataType) (bool, error)) error {
	return t.df.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
		return scanFn(ptr, t.row2map(row))
	})
}

func (t *Table) FullScanByIndex(
	indexName string,
	reverse bool,
	scanFn func(row map[string]types.DataType) (bool, error),
) error {
	idx, ok := t.indexes[indexName]
	if !ok {
		return fmt.Errorf("index not found => %v", indexName)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	return idx.Scan(index.ScanOptions{
		ScanOptions: bptree.ScanOptions{
			Reverse: reverse,
			Strict:  true,
		},
	}, func(key [][]byte, ptr allocator.Pointable) (bool, error) {
		return scanFn(t.Get(ptr))
	})
}

func (t *Table) CreateIndex(name *string, columns []string, opts IndexOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !opts.Primary && t.meta.PrimaryKey == nil {
		return errors.New("first index must be primary")
	}
	if opts.Primary && t.meta.PrimaryKey != nil {
		return errors.New("primary index already created")
	}
	if opts.AutoIncrement {
		if len(columns) != 1 {
			return fmt.Errorf("auto increment supported only fofr single column indexes")
		}
	}
	if name != nil {
		if _, ok := t.indexes[*name]; ok {
			return fmt.Errorf("index with name:'%s' already exists", *name)
		}
	}

	keySize := 0
	columnsList := make([]*column.Column, 0, len(columns))
	for _, columnName := range columns {
		if col, ok := t.meta.ColumnsMap[columnName]; !ok {
			return fmt.Errorf("unknown column:'%s'", columnName)
		} else if !col.Meta.IsFixedSize() {
			return fmt.Errorf("column must be of fixed size")
		} else {
			keySize += col.Meta.Size()
			columnsList = append(columnsList, col)
		}
	}

	if opts.AutoIncrement {
		if !columnsList[0].Meta.IsNumeric() {
			return fmt.Errorf("auto increment is supported for numeric types")
		}
	}

	if name == nil {
		name = new(string)
		*name = strings.Join(columns, "_")
		for i := 1; i < 100; i++ {
			postfix := fmt.Sprintf("_%d", i)
			if _, ok := t.indexes[*name + postfix]; !ok {
				*name += postfix
				break
			}
		}
	}

	if opts.Primary {
		opts.Uniq = true
	}

	suffixSize := 0
	suffixCols := 0
	if !opts.Primary {
		opts := t.indexes[*t.meta.PrimaryKey].Options()
		suffixSize = opts.MaxKeySize
		suffixCols = opts.KeyCols
	}

	indexOpts := &bptree.Options{
		KeyCols:       len(columns),
		MaxSuffixSize: suffixSize,
		SuffixCols:    suffixCols,
		MaxKeySize:    keySize,
		MaxValueSize:  allocator.PointerSize,
		Degree:        10,
		PageSize:      os.Getpagesize(),
		Uniq:          opts.Uniq,
		CacheSize:     10000,
	}

	tree, err := bptree.Open(t.indexPath(*name), indexOpts)
	if err != nil {
		return err
	}

	meta := &index.Meta{
		Name:    *name,
		Columns: columns,
		Uniq:    opts.Uniq,
		Options: indexOpts,
	}

	i := index.New(meta, t.df, tree, columnsList, opts.Uniq)
	t.indexes[*name] = i

	err = t.df.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
		return false, i.Insert(ptr, t.row2map(row))
	})
	if err != nil {
		i.Remove()
		return err
	}

	if opts.Primary {
		t.meta.PrimaryKey = name
	} else {
		i.SetPK(t.indexes[*t.meta.PrimaryKey])
	}

	t.meta.Indexes = append(t.meta.Indexes, meta)
	return t.writeMeta()
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
