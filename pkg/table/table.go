package table

import (
	"encoding/json"
	"fmt"
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/column"
	"go-dbms/pkg/data"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
	"os"
	"path"
	"strings"
)

const (
	metadataFileName = "metadata.json"
	dataFileName     = "data.dat"
	indexPath        = "./indexes"
)

type Table struct {
	path       string
	df         *data.DataFile
	meta       *metadata
	indexes    map[string]*index
}

func Open(tablePath string, opts *Options) (*Table, error) {
	table := &Table{
		path:    tablePath,
		indexes: map[string]*index{},
	}

	err := table.init(opts)
	if err != nil {
		return nil, err
	}

	dataOptions := data.DefaultOptions
	dataOptions.Columns = table.meta.Columns

	df, err := data.Open(
		path.Join(tablePath, dataFileName),
		&dataOptions,
	)
	if err != nil {
		return nil, err
	}

	table.df = df

	return table, nil
}

func (t *Table) Insert(values map[string]types.DataType) (*data.RecordPointer, error) {
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

	ptr, err := t.df.InsertRecord(dataToInsert)
	if err != nil {
		return nil, err
	}

	for _, i := range t.indexes {
		err := i.Insert(ptr, dataToInsertMap)
		if err != nil {
			return nil, err
		}
	}

	return ptr, nil
}

func (t *Table) FindByIndex(indexName string, operator string, values map[string]types.DataType) ([]map[string]types.DataType, error) {
	result, err := t.indexes[indexName].Find(
		values,
		operator,
		func(ptr *data.RecordPointer) (map[string]types.DataType, error) {
			row, err := t.Get(ptr)
			if err != nil {
				return nil, err
			}

			return row, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (t *Table) Get(ptr *data.RecordPointer) (map[string]types.DataType, error) {
	records, err := t.df.GetPage(ptr.PageId)
	if err != nil {
		return nil, err
	}

	return t.row2map(records[ptr.SlotId]), nil
}

func (t *Table) FullScan(scanFn func(ptr *data.RecordPointer, row map[string]types.DataType) (bool, error)) error {
	return t.df.Scan(func(ptr *data.RecordPointer, row []types.DataType) (bool, error) {
		return scanFn(ptr, t.row2map(row))
	})
}

func (t *Table) FullScanByIndex(
	indexName string,
	reverse bool,
	scanFn func(row map[string]types.DataType) (bool, error),
) error {
	return t.indexes[indexName].tree.Scan(nil, reverse, true, func(key [][]byte, val []byte) (bool, error) {
		ptr := &data.RecordPointer{}
		ptr.UnmarshalBinary(val)
		row, err := t.Get(ptr)
		if err != nil {
			return false, err
		}

		return scanFn(row)
	})
}

func (t *Table) CreateIndex(name *string, columns []string, uniq bool) error {
	if name != nil {
		if _, ok := t.indexes[*name]; ok {
			return fmt.Errorf("index with name:'%s' already exists", *name)
		}
	}

	keySize := len(columns) * 2 // 2 byte for length of each column
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
		ReadOnly:     false,
		FileMode:     0664,
		MaxKeySize:   keySize,
		MaxValueSize: data.RecordPointerSize,
		PageSize:     os.Getpagesize(),
	})
	if err != nil {
		return err
	}

	t.meta.Indexes = append(t.meta.Indexes, &metaIndex{
		Name:    *name,
		Columns: columns,
		Uniq:    uniq,
	})
	i := &index{
		tree:    tree,
		columns: columns,
		uniq:    uniq,
	}
	t.indexes[*name] = i

	return t.FullScan(func(ptr *data.RecordPointer, row map[string]types.DataType) (bool, error) {
		return false, i.Insert(ptr, row)
	})
}

func (t *Table) Columns() []*column.Column {
	return t.meta.Columns
}

func (t *Table) ColumnsMap() map[string]*column.Column {
	return t.meta.ColumnsMap
}

func (t *Table) Close() error {
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
		bpt, err := bptree.Open(
			t.indexPath(metaindex.Name),
			nil,
		)
		if err != nil {
			return err
		}

		t.indexes[metaindex.Name] = &index{
			tree:    bpt,
			columns: metaindex.Columns,
			uniq:    metaindex.Uniq,
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
		for _, c := range t.Columns() {
			if t.meta.Columns[i].Name == c.Name {
				rowMap[t.meta.Columns[i].Name] = data
				break
			}
		}
	}
	return rowMap
}
