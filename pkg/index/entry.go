package index

import (
	"go-dbms/pkg/types"

	allocator "github.com/vahagz/disk-allocator/heap"
)

type Entry struct{
	Ptr allocator.Pointable
	Row map[string]types.DataType
}
