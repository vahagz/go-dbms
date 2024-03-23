package table

import (
	"go-dbms/pkg/column"
)

type Engine string

const (
	InnoDB               Engine = "InnoDB"
	MergeTree            Engine = "MergeTree"
	SummingMergeTree     Engine = "SummingMergeTree"
	AggregatingMergeTree Engine = "AggregatingMergeTree"
)

// Options represents the configuration options for the table.
type Options struct {
	MetaFilePath, DataPath string
	Columns                []*column.Column
	Engine                 Engine
	Meta                   IMetadata
	NewMeta                func() IMetadata
}
