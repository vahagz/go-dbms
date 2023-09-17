package table

import (
	"encoding/json"
	"fmt"
	"go-dbms/pkg/bptree"
	data "go-dbms/pkg/slotted_data"
	"go-dbms/pkg/types"
	"os"
	"path"
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
	indexes      map[string]*bptree.BPlusTree
	columns      map[string]types.TypeCode
	columnsOrder []string
}

func Open(tablePath string, opts *Options) (*Table, error) {
	table := &Table{
		path:         tablePath,
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
	}

	ptr, err := t.df.InsertRecord(dataToInsert)
	if err != nil {
		return nil, err
	}

	return ptr, nil
}

func (t *Table) Get(ptr *data.RecordPointer) ([]types.DataType, error) {
	records, err := t.df.GetPage(ptr.PageId)
	if err != nil {
		return nil, err
	}

	return records[ptr.SlotId], nil
}

func (t *Table) Close() error {
	return t.df.Close()
}

func (t *Table) FullScan(scanFn func(ptr *data.RecordPointer, row []types.DataType) bool) error {
	return t.df.Scan(scanFn)
}

func (t *Table) init(opts *Options) error {
	err := t.readMeta(opts)
	if err != nil {
		return err
	}

	return t.readIndexes()
}

func (t *Table) readMeta(opts *Options) error {	
	metaPath := path.Join(t.path, metadataFileName)

  if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.meta = &metadata{
			Indexes:      []string{},
			PrimaryKey:   nil,
			ColumnsOrder: opts.ColumnsOrder,
			Columns:      opts.Columns,
		}

		metadataBytes, _ := json.Marshal(t.meta)
		return os.WriteFile(metaPath, metadataBytes, 0644)
  }

	metadataBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}

	t.meta = &metadata{}

	return json.Unmarshal(metadataBytes, t.meta)
}

func (t *Table) readIndexes() error {
	for _, index := range t.meta.Indexes {
		bpt, err := bptree.Open(
			path.Join(t.path, indexPath, fmt.Sprintf("%s.idx", index)),
			nil,
		)
		if err != nil {
			return err
		}

		t.indexes[index] = bpt
	}

	return nil
}
