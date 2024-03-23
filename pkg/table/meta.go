package table

import (
	"go-dbms/pkg/column"
	"go-dbms/pkg/index"
)

// metadata represents the metadata for the table stored in a json file.
type Metadata struct {
	Engine     Engine                    `json:"engine"`
	Indexes    []*index.Meta             `json:"indexes"`
	PrimaryKey string                    `json:"primary_key"`
	Columns    []*column.Column          `json:"columns"`
	ColumnsMap map[string]*column.Column `json:"-"`
}

type IMetadata interface {
	GetEngine() Engine
	SetEngine(v Engine)
	GetIndexes() []*index.Meta
	SetIndexes(v []*index.Meta)
	GetPrimaryKey() string
	SetPrimaryKey(v string)
	GetColumns() []*column.Column
	SetColumns(v []*column.Column)
	GetColumnsMap() map[string]*column.Column
	SetColumnsMap(v map[string]*column.Column)
}

func (m *Metadata) GetEngine() Engine { return m.Engine }
func (m *Metadata) SetEngine(v Engine) { m.Engine = v }
func (m *Metadata) GetIndexes() []*index.Meta { return m.Indexes }
func (m *Metadata) SetIndexes(v []*index.Meta) { m.Indexes = v }
func (m *Metadata) GetPrimaryKey() string { return m.PrimaryKey }
func (m *Metadata) SetPrimaryKey(v string) { m.PrimaryKey = v }
func (m *Metadata) GetColumns() []*column.Column { return m.Columns }
func (m *Metadata) SetColumns(v []*column.Column) { m.Columns = v }
func (m *Metadata) GetColumnsMap() map[string]*column.Column { return m.ColumnsMap }
func (m *Metadata) SetColumnsMap(v map[string]*column.Column) { m.ColumnsMap = v }
