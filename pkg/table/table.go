package table

import (
	"encoding/json"
	"fmt"
	"go-dbms/pkg/bptree"
	data "go-dbms/pkg/slotted_data"
	"go-dbms/pkg/types"
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
	path         string
	df           *data.DataFile
	meta         *metadata
	indexes      map[string]*index
	columns      map[string]types.TypeCode
	columnsOrder []string
}

func Open(tablePath string, opts *Options) (*Table, error) {
	table := &Table{
		path:         tablePath,
		indexes:      map[string]*index{},
		columns:      opts.Columns,
		columnsOrder: opts.ColumnsOrder,
	}

	err := table.init(opts)
	if err != nil {
		return nil, err
	}

	dataOptions := data.DefaultOptions
	dataOptions.Columns = table.columns

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
	
	if len(values) > len(t.columns) {
		return nil, fmt.Errorf("invalid columns count")
	}

	for columnName := range values {
		if _, ok := t.columns[columnName]; !ok {
			return nil, fmt.Errorf("unknown column while inserting => %s", columnName)
		}
	}

	for _, column := range t.columnsOrder {
		if data, ok := values[column]; !ok {
			dataToInsert = append(dataToInsert, types.Type(t.columns[column]))
		} else {
			dataToInsert = append(dataToInsert, data)
		}
		dataToInsertMap[column] = dataToInsert[len(dataToInsert)-1]
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

func (t *Table) FindOneByIndex(values map[string]types.DataType, indexName string) ([]types.DataType, error) {
	ptr, err := t.indexes[indexName].FindOne(values)
	if err != nil {
		return nil, err
	}
	return t.Get(ptr)
}

func (t *Table) Get(ptr *data.RecordPointer) ([]types.DataType, error) {
	records, err := t.df.GetPage(ptr.PageId)
	if err != nil {
		return nil, err
	}

	return records[ptr.SlotId], nil
}

func (t *Table) FullScan(scanFn func(ptr *data.RecordPointer, row []types.DataType) bool) error {
	return t.df.Scan(scanFn)
}

func (t *Table) CreateIndex(name *string, columns []string, uniq bool) error {
	if name != nil {
		if _, ok := t.indexes[*name]; ok {
			return fmt.Errorf("index with name:'%s' already exists", *name)
		}
	}

	for _, columnName := range columns {
		if _, ok := t.columns[columnName]; !ok {
			return fmt.Errorf("unknown column:'%s'", columnName)
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
		MaxKeySize:   8,
		MaxValueSize: 10,
		PageSize:     os.Getpagesize(),
		PreAlloc:     100,
	})
	if err != nil {
		return err
	}

	t.meta.Indexes = append(t.meta.Indexes, &metaIndex{
		Name:    *name,
		Columns: columns,
		Uniq:    uniq,
	})
	t.indexes[*name] = &index{
		tree:    tree,
		columns: columns,
		uniq:    uniq,
	}
	return nil
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
	err := t.readMeta(opts)
	if err != nil {
		return err
	}

	err = t.createDirs()
	if err != nil {
		return err
	}

	return t.readIndexes()
}

func (t *Table) metaPath() string {
	return path.Join(t.path, metadataFileName)
}

func (t *Table) indexPath(name string) string {
	return path.Join(t.path, indexPath, fmt.Sprintf("%s.idx", name))
}

func (t *Table) readMeta(opts *Options) error {	
	metaPath := t.metaPath()

  if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.meta = &metadata{
			Indexes:      []*metaIndex{},
			PrimaryKey:   nil,
			ColumnsOrder: opts.ColumnsOrder,
			Columns:      opts.Columns,
		}

		return t.writeMeta()
  }

	metadataBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}

	t.meta = &metadata{}

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
	dir := path.Join(t.path, indexPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}