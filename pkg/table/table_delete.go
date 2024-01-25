package table

import (
	"fmt"

	"go-dbms/pkg/index"
	"go-dbms/pkg/statement"
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) Delete(
	filter *statement.WhereStatement,
	scanFn func(row map[string]types.DataType) error,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.delete(t.Find(filter), t.indexes, scanFn)
}

func (t *Table) DeleteByIndex(
	name string,
	start, end *index.Filter,
	filter *statement.WhereStatement,
	scanFn func(row map[string]types.DataType) error,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	delIndex, ok := t.indexes[name]
	if !ok {
		return fmt.Errorf("index not found => '%s'", name)
	}

	return t.delete(
		delIndex.ScanEntries(start, end, filter),
		t.indexes,
		scanFn,
	)
}

func (t *Table) delete(
	entries []index.Entry,
	indexesToUpdate map[string]*index.Index,
	scanFn func(row map[string]types.DataType) error,
) error {
	for _, e := range entries {
		t.deleteRow(e.Ptr, e.Row, indexesToUpdate)
		if err := scanFn(t.row2pk(e.Row)); err != nil {
			return errors.Wrap(err, "failed to delete row")
		}
	}

	return nil
}

func (t *Table) deleteRow(
	ptr allocator.Pointable,
	row map[string]types.DataType,
	indexesToUpdate map[string]*index.Index,
) {
	for _, i := range indexesToUpdate {
		t.deleteIndex(i, ptr, row)
	}
	t.df.DeleteMem(ptr)
}

func (t *Table) deleteIndex(i *index.Index, ptr allocator.Pointable, row map[string]types.DataType) {
	if _, err := i.Delete(row, !t.isPK(i)); err != nil {
		panic(errors.Wrapf(err, "error while deleting from index '%s'", i.Meta().Name))
	}
}
