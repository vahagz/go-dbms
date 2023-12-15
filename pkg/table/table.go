package table

import (
	"encoding/json"
	"errors"
	"fmt"
	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/column"
	"go-dbms/pkg/data"
	"go-dbms/pkg/index"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
	"os"
	"path"
	"strings"
	"sync"
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
			dataToInsert = append(dataToInsert, types.Type(column.Meta))
		} else {
			dataToInsert = append(dataToInsert, data)
		}
		dataToInsertMap[column.Name] = dataToInsert[len(dataToInsert)-1]
	}

	ptr, err := t.df.Insert(dataToInsert)
	if err != nil {
		return nil, err
	}

	insertedIndexes := make([]string, 0, len(t.indexes))
	for i, index := range t.indexes {
		err := index.Insert(ptr, dataToInsertMap)
		if err != nil {
			t.df.DeleteMem(ptr)
			for _, indexName := range insertedIndexes {
				dataToDeleteMap := map[string]types.DataType{}
				for _, col := range t.indexes[indexName].Columns() {
					dataToDeleteMap[col] = dataToInsertMap[col]
				}
				err = t.indexes[indexName].Delete(dataToDeleteMap)
				if err != nil {
					return nil, err
				}
			}
			return nil, err
		}
		insertedIndexes = append(insertedIndexes, i)
	}

	return ptr, nil
}

func (t *Table) FindByIndex(
	indexName string,
	operator string,
	values map[string]types.DataType,
) (
	[]map[string]types.DataType,
	error,
) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.indexes[indexName].Find(
		values,
		operator,
		func(ptr allocator.Pointable) (map[string]types.DataType, error) {
			return t.Get(ptr), nil
		},
	)
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
	if name != nil {
		if _, ok := t.indexes[*name]; ok {
			return fmt.Errorf("index with name:'%s' already exists", *name)
		}
	}

	if !opts.Primary {
		columns = append(columns, t.indexes[*t.meta.PrimaryKey].Columns()...)
	}

	keySize := 0
	for _, columnName := range columns {
		if col, ok := t.meta.ColumnsMap[columnName]; !ok {
			return fmt.Errorf("unknown column:'%s'", columnName)
		} else if !col.Meta.IsFixedSize() {
			return fmt.Errorf("column must be of fixed size")
		} else {
			keySize += col.Meta.Size()
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

	tree, err := bptree.Open(t.indexPath(*name), &bptree.Options{
		KeyCols:      len(columns),
		MaxKeySize:   keySize,
		MaxValueSize: allocator.PointerSize,
		Degree:       200,
		PageSize:     os.Getpagesize(),
		Uniq:         opts.Uniq,
	})
	if err != nil {
		return err
	}

	if opts.Primary {
		t.meta.PrimaryKey = name
	}

	t.meta.Indexes = append(t.meta.Indexes, &metaIndex{
		Name:    *name,
		Columns: columns,
		Uniq:    opts.Uniq,
	})

	i := index.New(t.df, tree, columns, opts.Uniq)
	t.indexes[*name] = i

	return t.df.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
		return false, i.Insert(ptr, t.row2map(row))
	})
}

func (t *Table) Columns() []*column.Column {
	return t.meta.Columns
}

func (t *Table) ColumnsMap() map[string]*column.Column {
	return t.meta.ColumnsMap
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
			Indexes:    []*metaIndex{},
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
		//TODO: store index bptree options in table meta file and pass it on index opening		
		bpt, err := bptree.Open(
			t.indexPath(metaindex.Name),
			nil,
		)
		if err != nil {
			return err
		}

		t.indexes[metaindex.Name] = index.New(t.df, bpt, metaindex.Columns, metaindex.Uniq)
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
		for _, c := range t.meta.Columns {
			if t.meta.Columns[i].Name == c.Name {
				rowMap[t.meta.Columns[i].Name] = data
				break
			}
		}
	}
	return rowMap
}
