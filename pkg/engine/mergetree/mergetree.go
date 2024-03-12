package mergetree

import (
	"sync"

	"go-dbms/pkg/table"
)

type MergeTree struct {
	*table.Table
	Parts map[string]*table.Table

	path string
	mu   *sync.RWMutex
}

// func (mt *MergeTree) Insert(values map[string]types.DataType) (map[string]types.DataType, error) {
// 	name := uuid.NewString()
// 	table.Open()
// }

func (mt *MergeTree) Merge(t1, t2 *table.Table) {

}
