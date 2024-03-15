package table

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go-dbms/pkg/column"
	"go-dbms/pkg/data"
	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"
	"go-dbms/util/helpers"
	"go-dbms/util/stream"

	"github.com/vahagz/bptree"
	"golang.org/x/sync/errgroup"
)

const (
	MetadataFileName = "metadata.json"
	dataFileName     = "data"
	indexPath        = "./indexes"
)

type ITable interface {
	Insert(in stream.Reader[types.DataRow]) (stream.Reader[types.DataRow], *errgroup.Group)

	Find(filter *statement.WhereStatement) stream.Reader[index.Entry]
	ScanByIndex(name string, start, end *index.Filter) (stream.ReaderContinue[types.DataRow], error)
	FullScan() stream.ReaderContinue[types.DataRow]
	FullScanByIndex(indexName string, reverse bool) (stream.ReaderContinue[types.DataRow], error)

	Update(filter *statement.WhereStatement, updateValuesMap types.DataRow) stream.Reader[types.DataRow]
	UpdateByIndex(
		name string,
		start, end *index.Filter,
		filter *statement.WhereStatement,
		updateValuesMap types.DataRow,
	) (stream.Reader[types.DataRow], error)

	Delete(filter *statement.WhereStatement) stream.Reader[types.DataRow]
	DeleteByIndex(name string, start, end *index.Filter, filter *statement.WhereStatement) (stream.Reader[types.DataRow], error)

	PrepareSpace(rows int)

	Column(name string) *column.Column
	Columns() []*column.Column
	ColumnsMap() map[string]*column.Column

	CreateIndex(name *string, opts *index.IndexOptions) error
	HasIndex(name string) bool

	PrimaryColumns() []*column.Column
	PrimaryKey() string

	Engine() Engine
	Drop()
	Close()
}

type Table struct {
	DataPath, MetaFilePath string

	Mu, MetaMu *sync.RWMutex
	DF         *data.DataFile
	Meta       *Metadata
	Indexes    map[string]*index.Index
}

func Open(opts *Options) (ITable, error) {
	table := &Table{
		Mu:           &sync.RWMutex{},
		MetaMu:       &sync.RWMutex{},
		DataPath:     opts.DataPath,
		MetaFilePath: opts.MetaFilePath,
		Indexes:      map[string]*index.Index{},
		DF:           &data.DataFile{},
	}

	err := table.Init(opts)
	if err != nil {
		return nil, err
	}

	dataOptions := data.DefaultOptions
	dataOptions.Columns = table.Meta.Columns

	DF, err := data.Open(
		filepath.Join(table.DataPath, dataFileName),
		&dataOptions,
	)
	if err != nil {
		return nil, err
	}

	*table.DF = *DF
	return table, nil
}

func (t *Table) HasIndex(name string) bool {
	_, ok := t.Indexes[name]
	return ok
}

func (t *Table) Columns() []*column.Column {
	return t.Meta.Columns
}

func (t *Table) PrimaryKey() string {
	return t.Meta.PrimaryKey
}

func (t *Table) PrimaryColumns() []*column.Column {
	return t.Indexes[t.Meta.PrimaryKey].Columns()
}

func (t *Table) ColumnsMap() map[string]*column.Column {
	return t.Meta.ColumnsMap
}

func (t *Table) Column(name string) *column.Column {
	return t.Meta.ColumnsMap[name]
}

func (t *Table) PrepareSpace(rows int) {
	t.DF.PrepareSpace(uint32(rows * int(t.DF.HeapSize() / t.DF.Count())))
	for _, i := range t.Indexes {
		i.PrepareSpace(rows)
	}
}

func (t *Table) Engine() Engine {
	return t.Meta.Engine
}

func (t *Table) Drop() {
	t.Close()
	panic(filepath.Join(t.DataPath, ".."))
	// helpers.Must(os.RemoveAll(filepath.Join(t.DataPath, "..")))
}

func (t *Table) Close() {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	t.writeMeta()
	for _, index := range t.Indexes {
		index.Close()
	}
	t.DF.Close()
}

func (t *Table) Init(opts *Options) error {
	err := t.CreateDirs()
	if err != nil {
		return err
	}

	err = t.ReadMeta(opts)
	if err != nil {
		return err
	}

	return t.ReadIndexes()
}

func (t *Table) ReadMeta(opts *Options) error {
	defer func ()  {
		for _, c := range t.Meta.Columns {
			t.Meta.ColumnsMap[c.Name] = c
		}
	}()

	if opts.Meta != nil {
		t.Meta = opts.Meta
		return nil
	}

	if _, err := os.Stat(t.MetaFilePath); os.IsNotExist(err) {
		t.Meta = &Metadata{
			Engine:     opts.Engine,
			Indexes:    []*index.Meta{},
			Columns:    opts.Columns,
			ColumnsMap: map[string]*column.Column{},
		}

		t.writeMeta()
		return nil
  }

	metadataBytes, err := os.ReadFile(t.MetaFilePath)
	if err != nil {
		return err
	}

	t.Meta = &Metadata{
		ColumnsMap: map[string]*column.Column{},
	}

	return json.Unmarshal(metadataBytes, t.Meta)
}

func (t *Table) ReadIndexes() error {
	for _, metaindex := range t.Meta.Indexes {	
		bpt, err := bptree.Open(
			t.indexPath(metaindex.Name),
			metaindex.Options,
		)
		if err != nil {
			return err
		}

		columns := make([]*column.Column, 0, len(metaindex.Columns))
		for _, colName := range metaindex.Columns {
			columns = append(columns, t.Meta.ColumnsMap[colName])
		}

		t.Indexes[metaindex.Name] = index.New(
			metaindex,
			t.DF,
			bpt,
			columns,
			metaindex.Uniq,
		)
	}

	for k, i := range t.Indexes {
		if k != t.Meta.PrimaryKey {
			i.SetPK(t.Indexes[t.Meta.PrimaryKey])
		}
	}

	return nil
}

func (t *Table) indexPath(name string) string {
	return filepath.Join(t.DataPath, indexPath, name)
}

func (t *Table) writeMeta() {
	if t.MetaFilePath == "" {
		return
	}

	helpers.Must(os.WriteFile(
		t.MetaFilePath,
		helpers.MarshalJSON(t.Meta),
		0644,
	))
}

func (t *Table) CreateDirs() error {
	if err := helpers.CreateDir(t.DataPath); err != nil {
		return err
	}
	return helpers.CreateDir(filepath.Join(t.DataPath, indexPath))
}

func (t *Table) map2row(rowMap types.DataRow) []types.DataType {
	row := make([]types.DataType, 0, len(rowMap))
	for _, c := range t.Meta.Columns {
		if val, ok := rowMap[c.Name]; ok {
			row = append(row, val)
		}
	}
	return row
}

func (t *Table) Row2map(row []types.DataType) types.DataRow {
	rowMap := types.DataRow{}
	for i, data := range row {
		rowMap[t.Meta.Columns[i].Name] = data
	}
	return rowMap
}

func (t *Table) row2pk(row types.DataRow) types.DataRow {
	pkCols := t.Indexes[t.Meta.PrimaryKey].Columns()
	pkRow := make(types.DataRow, 1)
	for _, col := range pkCols {
		pkRow[col.Name] = row[col.Name]
	}
	return pkRow
}

func (t *Table) validateMap(row types.DataRow) error {
	if len(row) > len(t.Meta.Columns) {
		return fmt.Errorf("invalid columns count")
	}

	for columnName := range row {
		if _, ok := t.Meta.ColumnsMap[columnName]; !ok {
			return fmt.Errorf("unknown column while inserting => %s", columnName)
		}
	}
	return nil
}

func (t *Table) setDefaults(row types.DataRow) {
	t.MetaMu.Lock()
	defer t.MetaMu.Unlock()

	for _, column := range t.Meta.Columns {
		if _, ok := row[column.Name]; !ok {
			row[column.Name] = column.Meta.Default()
		}
	}
}

func (t *Table) isPK(i *index.Index) bool {
	return t.Meta.PrimaryKey == i.Meta().Name
}
