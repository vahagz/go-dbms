package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) DeleteByIndex(name string, start, end *index.Filter, filter *statement.WhereStatement) (
	[]map[string]types.DataType,
	error,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	index, ok := t.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index not found => '%s'", name)
	}

	entries := index.ScanEntries(start, end, filter)
	result := make([]map[string]types.DataType, 0, len(entries))

	for _, e := range entries {
		t.delete(e.Ptr, e.Row)
		result = append(result, t.row2pk(e.Row))
	}

	return result, nil
}

func (t *Table) delete(ptr allocator.Pointable, row map[string]types.DataType) {
	for _, i := range t.indexes {
		t.deleteIndex(i, ptr, row)
	}
	t.df.DeleteMem(ptr)
}

func (t *Table) deleteIndex(i *index.Index, ptr allocator.Pointable, row map[string]types.DataType) {
	if _, err := i.Delete(row, !t.isPK(i)); err != nil {
		panic(errors.Wrapf(err, "error while deleting from index '%s'", i.Meta().Name))
	}
}
