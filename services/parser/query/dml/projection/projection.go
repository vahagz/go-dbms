package projection

import (
	"go-dbms/pkg/types"
)

type ProjectionType uint8

const (
	AGGREGATOR ProjectionType = iota
	FUNCTION
	IDENTIFIER
)

type Projection struct {
	Alias     string
	Name      string
	Type      ProjectionType
	Arguments []*Projection
	Literal   types.DataType
}
